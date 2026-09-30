from pyspark.sql import functions as F

base = (
    df_tickets
    .filter((~F.col("channel").isin("chat", "web")) & (F.col("priority").isin("P4", "P2", "P1")))
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
        F.round((F.unix_timestamp(F.col("closed_at")) - F.unix_timestamp(F.col("opened_at"))) / 3600, 1).alias("hours_opened_at_to_closed_at"),
    )
)

filtered = (
    base.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.csat") >= 2) & (F.col("t.channel") == "email"), "left_anti")
    .filter(F.col("csat") < 4)
)

tagged = (
    filtered
    .select(
        "*",
        (F.col("opened_at") + F.expr("INTERVAL 30 MINUTES")).alias("opened_at_plus_30m"),
        F.round((F.unix_timestamp(F.col("opened_at")) - F.unix_timestamp(F.col("closed_at"))) / 60, 1).alias("minutes_closed_at_to_opened_at"),
        (F.col("closed_at") + F.expr("INTERVAL 1 DAYS")).alias("closed_at_plus_1d"),
    )
)

result = (
    tagged
    .filter(F.col("hours_opened_at_to_closed_at") < 400.0)
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        "tags",
        "hours_opened_at_to_closed_at",
        "opened_at_plus_30m",
        "minutes_closed_at_to_opened_at",
    )
)
