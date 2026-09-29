from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority = Window.partitionBy("priority").orderBy("closed_at", "id")
w_priority_running = Window.partitionBy("priority").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_tickets = (
    df_tickets
    .filter(F.col("priority") == "P3")
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "tags",
    )
)

with_prev = base_tickets.withColumn("prev_closed_at", F.lag("closed_at").over(w_priority))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_closed_at").isNull() | (F.col("closed_at") > F.col("prev_closed_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_priority_running))

per_session = (
    sessionized
    .groupBy("priority", "burst_id")
    .agg(
        F.min("closed_at").alias("session_start"),
        F.max("closed_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("closed_at")) - F.unix_timestamp(F.min("closed_at"))) / 60, 1).alias("duration_min"),
    )
)

totals = (
    per_session
    .groupBy(
        "events",
        F.date_format("session_start", "yyyy-MM").alias("session_start_ym"),
    )
    .agg(
        F.round(F.avg("events"), 2).alias("avg_events"),
        F.min("duration_min").alias("min_duration_min"),
        F.sum(F.when(F.col("priority").isin("P4", "P3", "P2"), F.col("burst_id"))).alias("burst_id_priority_in_p4_p3_p2"),
        F.round(F.stddev("burst_id"), 2).alias("stddev_burst_id"),
        F.sum("burst_id").alias("total_burst_id"),
        F.sum("duration_min").alias("total_duration_min"),
    )
    .filter(F.col("total_duration_min") > 84.0)
)

result = (
    totals
    .select(
        "events",
        "session_start_ym",
        "avg_events",
        "min_duration_min",
        "burst_id_priority_in_p4_p3_p2",
        "stddev_burst_id",
        "total_duration_min",
        F.lower(F.trim(F.col("session_start_ym"))).alias("session_start_ym_clean"),
        F.when(F.col("min_duration_min") >= 192.0, "HIGH").when(F.col("min_duration_min") >= 72.0, "MEDIUM").otherwise("LOW").alias("min_duration_min_tier"),
    )
    .orderBy(
        "burst_id_priority_in_p4_p3_p2",
        "stddev_burst_id",
        "events",
        "session_start_ym",
    )
    .limit(10)
)
