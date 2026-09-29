from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority = Window.partitionBy("priority").orderBy("opened_at", "id")
w_priority_running = Window.partitionBy("priority").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = (
    df_tickets
    .filter(F.col("opened_at") < F.lit("2025-03-02 10:00:00").cast("timestamp"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.concat(F.col("channel"), F.lit("#"), F.col("csat").cast("string")).alias("channel_ref"),
    )
)

with_prev = scoped.withColumn("prev_opened_at", F.lag("opened_at").over(w_priority))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_opened_at").isNull() | ((F.unix_timestamp("opened_at") - F.unix_timestamp("prev_opened_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_priority_running))

sessions = (
    sessionized
    .groupBy("priority", "session_id")
    .agg(
        F.min("opened_at").alias("session_start"),
        F.max("opened_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("requester_id").alias("session_requester_id"),
        F.round((F.unix_timestamp(F.max("opened_at")) - F.unix_timestamp(F.min("opened_at"))) / 60, 1).alias("duration_min"),
    )
)

result = (
    sessions
    .rollup("event_count", "priority", "session_id")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.array_sort(F.collect_set("priority")).alias("priority_list"),
        F.sum(F.when(F.col("priority") == "P1", F.col("duration_min"))).alias("duration_min_priority_p1"),
    )
)
