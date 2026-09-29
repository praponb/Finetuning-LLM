from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type = Window.partitionBy("event_type").orderBy("event_at", "id")
w_event_type_running = Window.partitionBy("event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_session_no = Window.partitionBy("session_no")
w_all_by_session_end_ties = Window.orderBy("session_end")

filtered = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_prev = filtered.withColumn("prev_event_at", F.lag("event_at").over(w_event_type))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_event_type_running))

per_session = (
    sessionized
    .groupBy("event_type", "session_no")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

with_windows = (
    per_session
    .select(
        "*",
        F.count(F.lit(1)).over(w_session_no).alias("rows_in_session_no"),
        F.round(F.col("session_no") / F.nullif(F.sum("session_no").over(w_session_no), F.lit(0)), 4).alias("session_no_share_of_session_no"),
        F.dense_rank().over(w_all_by_session_end_ties).alias("session_end_dense_rank"),
    )
)

rollup_base = (
    with_windows
    .groupBy(
        "event_type",
        F.date_trunc("month", "session_start").alias("session_start_month"),
    )
    .agg(
        F.count(F.when(~F.col("event_type").isin("resolved", "reopened"), 1)).alias("n_event_type_not_in_resolved_r"),
        F.round(F.sum("session_no_share_of_session_no") / F.nullif(F.sum("session_end_dense_rank"), F.lit(0)), 3).alias("session_no_share_of_session_no_to_session_end_de"),
        F.sum(F.when(F.col("event_type").isNotNull(), 1).otherwise(0)).alias("n_event_type_is_not_null"),
        F.sum("duration_min").alias("total_duration_min"),
    )
)

result = rollup_base.select("event_type", "n_event_type_is_not_null")
