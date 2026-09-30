from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type = Window.partitionBy("event_type").orderBy("event_at", "id")
w_event_type_running = Window.partitionBy("event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_session_id = Window.partitionBy("session_id")
w_session_end_month_by_session = Window.partitionBy(F.date_trunc("month", "session_end")).orderBy("session_end")
w_session_end_month_by_session_2 = Window.partitionBy(F.date_trunc("month", "session_end")).orderBy("session_end", "event_type", "session_id")
w_session_end_month_by_session_3 = Window.partitionBy(F.date_trunc("month", "session_end")).orderBy("session_end", "event_type", "session_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

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
    .withColumn("new_session_flag", F.when(F.col("prev_event_at").isNull() | ((F.unix_timestamp("event_at") - F.unix_timestamp("prev_event_at")) / 60 > 45), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_event_type_running))

session_summary = (
    sessionized
    .groupBy("event_type", "session_id")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

metrics = (
    session_summary
    .select(
        "*",
        F.round(F.avg("session_id").over(w_session_id), 2).alias("session_id_avg_session_id"),
        F.round(F.cume_dist().over(w_session_end_month_by_session), 3).alias("session_end_cume_dist"),
        F.lead("duration_min", 2).over(w_session_end_month_by_session_2).alias("next2_duration_min"),
        F.last("duration_min").over(w_session_end_month_by_session_3).alias("last_duration_min"),
        F.count(F.lit(1)).over(w_session_id).alias("rows_in_session_id"),
    )
)

per_group = (
    metrics
    .groupBy(
        "event_type",
        F.when(F.col("rows_in_session_id") >= 14, "HIGH").when(F.col("rows_in_session_id") >= 8, "MEDIUM").otherwise("LOW").alias("rows_in_session_id_tier"),
    )
    .agg(
        F.round(F.sum("session_id_avg_session_id") / F.nullif(F.sum("rows_in_session_id"), F.lit(0)), 3).alias("session_id_avg_session_id_to_rows_in_session_id_"),
        F.round(F.sum("events") / F.nullif(F.sum("rows_in_session_id"), F.lit(0)), 3).alias("events_to_rows_in_session_id_ratio"),
    )
)

result = (
    per_group
    .groupBy("event_type")
    .pivot("rows_in_session_id_tier", ["LOW", "MEDIUM"])
    .agg(F.max("session_id_avg_session_id_to_rows_in_session_id_").alias("max_v"), F.sum("session_id_avg_session_id_to_rows_in_session_id_").alias("sum_v"))
    .orderBy("event_type")
)
