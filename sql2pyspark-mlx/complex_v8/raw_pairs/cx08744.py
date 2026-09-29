from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_or_unknown_channel = Window.partitionBy("priority_or_unknown", "channel").orderBy(F.col("requester_id").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_priority_or_unknown_channel_2 = Window.partitionBy("priority_or_unknown", "channel").orderBy(F.col("requester_id").desc_nulls_first())
w_csat = Window.partitionBy("csat")
w_priority_or_unknown_channel_3 = Window.partitionBy("priority_or_unknown", "channel").orderBy(F.col("requester_id").desc_nulls_first(), "id")

recent_tickets = (
    df_tickets
    .filter((F.col("opened_at") < F.lit("2025-03-02 09:00:00").cast("timestamp")) & (F.col("channel").isin("chat", "phone")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        F.round(F.col("csat") / F.nullif(F.col("requester_id"), F.lit(0)), 3).alias("csat_per_requester_id"),
        F.when(F.col("priority").isin("P3", "P2"), "group_a").when(F.col("priority") == "P1", "group_b").otherwise("other").alias("priority_group"),
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
    )
)

enriched = (
    recent_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.priority",
        "s.channel",
        "s.csat",
        "s.csat_per_requester_id",
        "s.priority_group",
        "s.priority_or_unknown",
        F.col("su.team").alias("agent_team"),
        "su.agent_name",
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

ranked = (
    enriched
    .select(
        "*",
        F.first("opened_at", ignorenulls=True).over(w_priority_or_unknown_channel).alias("first_known_opened_at"),
        F.first("agent_hired_on", ignorenulls=True).over(w_priority_or_unknown_channel).alias("first_known_agent_hired_on"),
        F.first("requester_id", ignorenulls=True).over(w_priority_or_unknown_channel).alias("first_known_requester_id"),
        F.round(F.cume_dist().over(w_priority_or_unknown_channel_2), 3).alias("requester_id_cume_dist"),
        F.round(F.avg("requester_id").over(w_csat), 2).alias("csat_avg_requester_id"),
        F.lag("opened_at").over(w_priority_or_unknown_channel_3).alias("prev_opened_at"),
    )
)

result = (
    ranked
    .groupBy("opened_at")
    .pivot("priority_or_unknown", ["P2", "P1"])
    .agg(F.max("csat_avg_requester_id"))
)
