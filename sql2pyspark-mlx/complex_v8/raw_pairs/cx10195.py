from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel_priority_by_agent_hi = Window.partitionBy("channel", "priority").orderBy("agent_hired_on", "id")
w_priority_agent_tier_by_opene = Window.partitionBy("priority", "agent_tier").orderBy(F.col("opened_at").desc(), "id")

filtered_tickets = (
    df_tickets
    .filter((F.col("opened_at") < F.lit("2025-03-02 09:00:00").cast("timestamp")) | (F.col("closed_at") >= F.lit("2025-01-27 00:00:00").cast("timestamp")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
    )
)

with_details = (
    filtered_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.length(F.trim(F.col("su.agent_name"))) >= 5)
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.priority",
        "s.channel",
        "s.tags",
        "su.agent_name",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.tier").alias("agent_tier"),
    )
)

ranked = (
    with_details
    .select(
        "id",
        "opened_at",
        "priority",
        "channel",
        "tags",
        "agent_name",
        "agent_hired_on",
        "agent_tier",
        F.rank().over(w_channel_priority_by_agent_hi).alias("agent_hired_on_rank"),
        F.row_number().over(w_channel_priority_by_agent_hi).alias("rn"),
        F.lag("opened_at").over(w_priority_agent_tier_by_opene).alias("prev_opened_at"),
        F.lead("opened_at").over(w_priority_agent_tier_by_opene).alias("next_opened_at"),
    )
)

result = (
    ranked
    .groupingSets(
        [["channel", "agent_tier"], ["channel"]],
        "channel", "agent_tier",
    )
    .agg(
        F.round(F.stddev("agent_hired_on_rank"), 2).alias("stddev_agent_hired_on_rank"),
    )
)
