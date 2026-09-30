from pyspark.sql import functions as F

valid = (
    df_tickets
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        F.concat_ws("-", F.col("channel"), F.col("priority")).alias("channel_priority_label"),
        F.when(F.col("priority").isin("P3", "P1"), "group_a").when(F.col("priority") == "P2", "group_b").otherwise("other").alias("priority_group"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("closed_at")).alias("days_since_closed_at"),
    )
)

with_parent = (
    valid.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.priority",
        "s.channel",
        "s.csat",
        "s.channel_priority_label",
        "s.days_since_closed_at",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.tier").alias("agent_tier"),
    )
)

derived = with_parent.select("*", F.quarter(F.col("opened_at")).alias("opened_at_quarter"))

result = (
    derived
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        "channel_priority_label",
        "agent_hired_on",
        "agent_tier",
        "opened_at_quarter",
        F.greatest(F.col("opened_at_quarter"), F.col("csat")).alias("greatest_opened_at_quarter_csat"),
        (F.col("opened_at_quarter") - F.col("days_since_closed_at")).alias("opened_at_quarter_minus_days_since_closed_at"),
    )
    .orderBy(F.col("opened_at_quarter").desc(), F.col("agent_id").desc())
)
