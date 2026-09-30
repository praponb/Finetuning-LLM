from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority = Window.partitionBy("priority").orderBy("closed_at", "id")
w_priority_running = Window.partitionBy("priority").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_tickets
    .filter(F.col("closed_at") >= F.lit("2025-01-26 18:00:00").cast("timestamp"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "csat",
        "tags",
    )
)

with_prev = base.withColumn("prev_closed_at", F.lag("closed_at").over(w_priority))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_closed_at").isNull() | (F.col("closed_at") > F.col("prev_closed_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_priority_running))

session_summary = (
    sessionized
    .groupBy("priority", "session_id")
    .agg(
        F.min("closed_at").alias("session_start"),
        F.max("closed_at").alias("session_end"),
        F.count("*").alias("events"),
        F.sum("csat").alias("session_csat"),
        F.round((F.unix_timestamp(F.max("closed_at")) - F.unix_timestamp(F.min("closed_at"))) / 60, 1).alias("duration_min"),
    )
)

annotated = (
    session_summary
    .select(
        "priority",
        "session_id",
        "session_start",
        "session_end",
        "events",
        "session_csat",
        "duration_min",
        F.when(F.col("priority").isin("P1", "P2"), "group_a").when(F.col("priority") == "P3", "group_b").otherwise("other").alias("priority_group"),
    )
)

result = (
    annotated
    .filter(F.col("priority") == "P4")
    .select(
        "priority",
        "session_id",
        "session_start",
        "session_end",
        "events",
        "session_csat",
        "duration_min",
        "priority_group",
    )
    .orderBy(F.col("priority_group").asc_nulls_last(), F.col("duration_min").desc())
)
