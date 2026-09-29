from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("channel", "channel_or_unknown").orderBy(F.col("requester_id").desc(), F.col("id").desc())

filtered = (
    df_tickets
    .filter(F.array_contains(F.col("tags"), "login"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "csat",
        "tags",
        F.concat(F.col("priority"), F.lit("#"), F.col("csat").cast("string")).alias("priority_ref"),
        F.coalesce(F.col("channel"), F.lit("UNKNOWN")).alias("channel_or_unknown"),
    )
)

latest_per_key = (
    filtered
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_activity = (
    latest_per_key.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (~F.col("t.channel").isin("email", "phone")), "left_semi")
)

joined = (
    with_activity.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.tier").isin("T1", "T3")), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.tags",
        "s.priority_ref",
        "s.channel_or_unknown",
        F.col("su.team").alias("agent_team"),
        F.col("su.tier").alias("agent_tier"),
        "su.agent_name",
    )
)

scored = (
    joined
    .filter(F.col("channel_or_unknown") == "chat")
    .withColumn("opened_at_day", F.date_trunc("day", F.col("opened_at")))
)

result = (
    scored
    .select(
        "id",
        "agent_id",
        "tags",
        "priority_ref",
        "channel_or_unknown",
        "agent_team",
        "agent_name",
        "opened_at_day",
        F.abs(F.col("requester_id") - 16).alias("requester_id_distance"),
        F.length(F.trim(F.col("agent_team"))).alias("agent_team_len"),
    )
    .orderBy(F.col("agent_team").desc())
)
