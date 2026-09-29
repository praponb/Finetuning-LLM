from pyspark.sql import functions as F

valid = (
    df_tickets
    .filter(F.col("csat") > 3)
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
        F.date_format(F.col("opened_at"), "yyyy-MM-dd HH:00").alias("opened_at_label"),
        F.when(F.col("csat") >= 4, "large").when(F.col("csat") >= 3, "medium").otherwise("small").alias("csat_tier"),
    )
)

joined_agent = (
    valid.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.priority",
        "s.channel",
        "s.csat",
        "s.tags",
        "s.opened_at_label",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.team").alias("agent_team"),
        F.col("su.tier").alias("agent_tier"),
    )
)

derived = (
    joined_agent
    .filter(~F.col("agent_tier").isin("T2", "T3"))
    .select("*", F.length(F.trim(F.col("priority"))).alias("priority_len"))
)

result = (
    derived
    .groupBy("opened_at")
    .pivot("priority", ["P1", "P4", "P2"])
    .agg(F.min("csat"))
)
