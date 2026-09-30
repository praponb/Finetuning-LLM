from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id").orderBy("opened_at", "id")
w_agent_id_running = Window.partitionBy("agent_id").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_tickets
    .filter(F.hour(F.col("closed_at")).between(9, 12))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "csat",
        "tags",
        F.array_contains(F.col("tags"), "refund").alias("has_refund"),
    )
)

with_prev = filtered.withColumn("prev_opened_at", F.lag("opened_at").over(w_agent_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_opened_at").isNull() | ((F.unix_timestamp("opened_at") - F.unix_timestamp("prev_opened_at")) / 60 > 120), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_agent_id_running))

combined_details = (
    sessionized.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.channel",
        "s.csat",
        "s.tags",
        "s.has_refund",
        "s.prev_opened_at",
        "s.starts_session",
        "s.burst_id",
        F.col("su.hired_on").alias("agent_hired_on"),
        "su.agent_name",
    )
)

per_group = (
    combined_details
    .groupBy(
        "channel",
        F.to_date("prev_opened_at").alias("prev_opened_at_day"),
        "has_refund",
    )
    .agg(
        F.round(F.avg("burst_id"), 2).alias("avg_burst_id"),
        F.sum("starts_session").alias("total_starts_session"),
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max("prev_opened_at").alias("last_prev_opened_at"),
    )
    .filter(F.col("cnt") >= 2)
)

result = (
    per_group
    .select(
        "channel",
        "prev_opened_at_day",
        "has_refund",
        "cnt",
    )
    .orderBy(F.col("prev_opened_at_day").desc_nulls_first())
)
