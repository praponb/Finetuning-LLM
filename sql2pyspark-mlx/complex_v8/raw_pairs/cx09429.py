from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority = Window.partitionBy("priority").orderBy("closed_at", "id")
w_priority_running = Window.partitionBy("priority").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_tickets
    .filter(F.col("opened_at") < F.lit("2025-02-24 14:00:00").cast("timestamp"))
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
    )
)

with_prev = prepared.withColumn("prev_closed_at", F.lag("closed_at").over(w_priority))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_closed_at").isNull() | ((F.unix_timestamp("closed_at") - F.unix_timestamp("prev_closed_at")) / 60 > 45), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_priority_running))

per_session = (
    sessionized
    .groupBy("priority", "session_id")
    .agg(
        F.min("closed_at").alias("session_start"),
        F.max("closed_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("closed_at")) - F.unix_timestamp(F.min("closed_at"))) / 60, 1).alias("duration_min"),
    )
)

rollup_base = (
    per_session
    .filter(~F.col("priority").isin("P2", "P1"))
    .groupBy(F.date_trunc("month", "session_start").alias("session_start_month"))
    .agg(
        F.sum("session_id").alias("total_session_id"),
        F.array_sort(F.collect_set("priority")).alias("priority_list"),
    )
)

result = (
    rollup_base
    .select(
        "session_start_month",
        "total_session_id",
        "priority_list",
        F.size(F.col("priority_list")).alias("priority_list_count"),
    )
    .orderBy(
        F.col("priority_list_count").desc_nulls_first(),
        F.col("total_session_id").desc(),
    )
)
