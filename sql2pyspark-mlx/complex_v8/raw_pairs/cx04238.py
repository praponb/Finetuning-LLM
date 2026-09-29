from pyspark.sql import functions as F

valid = (
    df_tickets
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "csat",
        "tags",
        F.date_trunc("day", F.col("opened_at")).alias("opened_at_day"),
        (F.col("requester_id") - F.col("csat")).alias("requester_id_minus_csat"),
    )
)

with_details_agent = (
    valid.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.priority",
        "s.tags",
        "s.opened_at_day",
        "s.requester_id_minus_csat",
        "su.agent_name",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.tier").alias("agent_tier"),
    )
)

annotated = (
    with_details_agent
    .filter(F.col("requester_id") <= 23)
    .select(
        "id",
        "agent_id",
        "priority",
        "tags",
        "opened_at_day",
        "requester_id_minus_csat",
        "agent_name",
        F.dayofweek(F.col("agent_hired_on")).alias("agent_hired_on_dayofweek"),
    )
)

screened = (
    annotated.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.channel") == "web") & (F.col("t.csat") < 5), "left_semi")
    .filter(F.hour(F.col("opened_at_day")).between(7, 12))
)

result = (
    screened
    .groupBy("agent_id")
    .pivot("priority", ["P1", "P3", "P4", "P2"])
    .agg(F.sum("agent_hired_on_dayofweek"))
    .orderBy("agent_id")
)
