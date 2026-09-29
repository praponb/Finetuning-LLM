from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority = Window.partitionBy("priority").orderBy("opened_at", "id")
w_priority_running = Window.partitionBy("priority").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_tickets
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        "tags",
        (F.floor(F.col("requester_id") / 5) * 5).alias("requester_id_bucket"),
    )
)

with_prev = base.withColumn("prev_opened_at", F.lag("opened_at").over(w_priority))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_opened_at").isNull() | ((F.unix_timestamp("opened_at") - F.unix_timestamp("prev_opened_at")) / 60 > 60), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_priority_running))

session_summary = (
    sessionized
    .groupBy("priority", "burst_id")
    .agg(
        F.min("opened_at").alias("session_start"),
        F.max("opened_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("opened_at")) - F.unix_timestamp(F.min("opened_at"))) / 60, 1).alias("duration_min"),
    )
)

filtered = (
    session_summary
    .crossJoin(session_summary.agg(F.avg("burst_id").alias("overall_avg_burst_id")))
    .filter(F.col("burst_id") > F.col("overall_avg_burst_id"))
    .drop("overall_avg_burst_id")
)

result = (
    filtered
    .filter(F.col("burst_id") < 5)
    .select(
        "priority",
        "events",
        "duration_min",
        F.length(F.trim(F.col("priority"))).alias("priority_len"),
    )
    .orderBy(F.col("duration_min").desc(), F.col("priority").asc_nulls_last())
)
