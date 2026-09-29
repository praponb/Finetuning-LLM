from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id").orderBy("opened_at", "id")
w_agent_id_running = Window.partitionBy("agent_id").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_tickets = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "csat",
        "tags",
        F.when(F.col("priority").isin("P2", "P3"), "group_a").when(F.col("priority") == "P1", "group_b").otherwise("other").alias("priority_group"),
    )
)

with_prev = scoped_tickets.withColumn("prev_opened_at", F.lag("opened_at").over(w_agent_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_opened_at").isNull() | (F.col("opened_at") > F.col("prev_opened_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_agent_id_running))

priority_group_stats = (
    sessionized
    .groupBy("priority_group")
    .agg(F.avg("requester_id").alias("avg_requester_id"))
)

compared = (
    sessionized.alias("s")
    .join(priority_group_stats.alias("g"), F.col("g.priority_group") == F.col("s.priority_group"), "inner")
    .filter(F.col("s.requester_id") > F.col("g.avg_requester_id"))
    .select(
        "s.*",
        "g.avg_requester_id",
        F.round(F.col("s.requester_id") - F.col("g.avg_requester_id"), 2).alias("requester_id_minus_avg"),
    )
)

result = (
    compared
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "csat",
        "starts_session",
        "burst_id",
        "avg_requester_id",
        "requester_id_minus_avg",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("prev_opened_at")).alias("days_since_prev_opened_at"),
    )
    .orderBy(F.col("requester_id").desc(), "id")
    .limit(20)
)
