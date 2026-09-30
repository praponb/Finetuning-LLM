from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("channel_or_unknown").orderBy(F.col("requester_id").desc(), F.col("id").desc())

recent_tickets = (
    df_tickets
    .filter((F.hour(F.col("closed_at")).between(6, 8)) & (F.col("agent_id") >= 9))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        F.date_trunc("year", F.col("opened_at")).alias("opened_at_year"),
        F.coalesce(F.col("channel"), F.lit("UNKNOWN")).alias("channel_or_unknown"),
    )
)

deduped = (
    recent_tickets
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

with_details = (
    deduped.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(~F.col("su.team").isin("VIP", "Onboarding"))
    .select(
        "s.*",
        F.col("su.team").alias("agent_team"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

tagged = (
    with_details
    .select(
        "*",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("opened_at_year")).alias("days_since_opened_at_year"),
    )
)

derived = (
    tagged
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "channel",
        "opened_at_year",
        "channel_or_unknown",
        "agent_hired_on",
        "days_since_opened_at_year",
        F.concat(F.col("channel_or_unknown"), F.lit("#"), F.col("requester_id").cast("string")).alias("channel_or_unknown_ref"),
    )
)

with_activity = (
    derived.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.csat") > 2), "left_semi")
)

result = (
    with_activity
    .filter(F.col("requester_id") <= 19)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "channel",
        "agent_hired_on",
        "days_since_opened_at_year",
        "channel_or_unknown_ref",
        F.substring(F.trim(F.col("channel_or_unknown_ref")), 1, 2).alias("channel_or_unknown_ref_prefix"),
        F.coalesce(F.col("channel"), F.lit("UNKNOWN")).alias("channel_or_unknown_2"),
    )
    .orderBy("channel_or_unknown_ref_prefix")
)
