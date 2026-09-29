from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor = Window.partitionBy("actor").orderBy("event_at", "id")
w_actor_running = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_event_count_actor_is = Window.orderBy(F.col("event_count_actor_is_not_null").desc(), "actor")
w_all_by_event_count_actor_is_2 = Window.orderBy(F.col("event_count_actor_is_not_null").desc(), "actor").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base = (
    df_ticket_events
    .filter(F.col("event_type") == "replied")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_format(F.col("event_at"), "MMM yyyy").alias("event_at_label"),
    )
)

with_prev = base.withColumn("prev_event_at", F.lag("event_at").over(w_actor))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_event_at").isNull() | ((F.unix_timestamp("event_at") - F.unix_timestamp("prev_event_at")) / 60 > 90), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_actor_running))

session_summary = (
    sessionized
    .groupBy("actor", "burst_id")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

rollup_base = (
    session_summary
    .filter(F.col("duration_min") < 192.0)
    .groupBy("actor")
    .agg(
        F.sum(F.when(F.col("actor").isNotNull(), F.col("event_count")).otherwise(0)).alias("event_count_actor_is_not_null"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") >= 2)
)

ranked = (
    rollup_base
    .select(
        "actor",
        F.dense_rank().over(w_all_by_event_count_actor_is).alias("event_count_actor_is_not_null_dense_rank"),
        F.nth_value("row_count", 2).over(w_all_by_event_count_actor_is_2).alias("second_row_count"),
        F.ntile(4).over(w_all_by_event_count_actor_is).alias("event_count_actor_is_not_null_quartile"),
        F.lag("row_count").over(w_all_by_event_count_actor_is).alias("prev_row_count"),
        F.lag("row_count", 1, 0).over(w_all_by_event_count_actor_is).alias("prev_row_count_2"),
        F.lag("row_count", 2).over(w_all_by_event_count_actor_is).alias("prev2_row_count"),
    )
)

result = (
    ranked
    .select(
        "actor",
        "event_count_actor_is_not_null_dense_rank",
        "prev_row_count_2",
        "prev2_row_count",
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
    )
    .orderBy(F.col("actor").asc())
)
