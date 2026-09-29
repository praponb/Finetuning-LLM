from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_opened_at_year = Window.partitionBy(F.year("opened_at"))

prepared_tickets = (
    df_tickets
    .filter(F.col("opened_at") < F.lit("2025-02-18 07:00:00").cast("timestamp"))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "channel",
        "tags",
        F.lower(F.regexp_replace(F.trim(F.col("priority")), " ", "_")).alias("priority_slug"),
        F.least(F.col("csat"), F.col("requester_id")).alias("least_csat_requester_id"),
        F.hour(F.col("closed_at")).alias("closed_at_hour"),
    )
)

with_trends = (
    prepared_tickets
    .select(
        "*",
        F.round(F.avg("least_csat_requester_id").over(w_opened_at_year), 2).alias("opened_at_year_avg_least_csat_requester_id"),
        F.round(F.avg("closed_at_hour").over(w_opened_at_year), 2).alias("opened_at_year_avg_closed_at_hour"),
        F.round(F.col("least_csat_requester_id") / F.nullif(F.sum("least_csat_requester_id").over(w_opened_at_year), F.lit(0)), 4).alias("least_csat_requester_id_share_of_opened_at_year"),
        F.size(F.collect_set("channel").over(w_opened_at_year)).alias("distinct_channel_in_opened_at_year"),
    )
)

joined = (
    with_trends.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.channel",
        "s.tags",
        "s.priority_slug",
        "s.least_csat_requester_id",
        "s.opened_at_year_avg_least_csat_requester_id",
        "s.opened_at_year_avg_closed_at_hour",
        "s.least_csat_requester_id_share_of_opened_at_year",
        "s.distinct_channel_in_opened_at_year",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.team").alias("agent_team"),
    )
)

agent_tier_stats = (
    joined
    .groupBy("agent_tier")
    .agg(F.avg("least_csat_requester_id_share_of_opened_at_year").alias("avg_least_csat_requester_id_share_of_opened_at_y"))
)

above_average = (
    joined.alias("s")
    .join(agent_tier_stats.alias("g"), F.col("g.agent_tier") == F.col("s.agent_tier"), "inner")
    .filter(F.col("s.least_csat_requester_id_share_of_opened_at_year") > F.col("g.avg_least_csat_requester_id_share_of_opened_at_y"))
    .select(
        "s.*",
        "g.avg_least_csat_requester_id_share_of_opened_at_y",
        F.round(F.col("s.least_csat_requester_id_share_of_opened_at_year") - F.col("g.avg_least_csat_requester_id_share_of_opened_at_y"), 2).alias("least_csat_requester_id_share_of_opened_at_year_"),
    )
)

result = (
    above_average
    .select(
        "id",
        "agent_id",
        "channel",
        "priority_slug",
        "least_csat_requester_id",
        "opened_at_year_avg_closed_at_hour",
        "least_csat_requester_id_share_of_opened_at_year",
        "distinct_channel_in_opened_at_year",
        "agent_tier",
        "avg_least_csat_requester_id_share_of_opened_at_y",
        "least_csat_requester_id_share_of_opened_at_year_",
        F.initcap(F.trim(F.col("priority_slug"))).alias("priority_slug_clean"),
        F.array_contains(F.col("tags"), "upgrade").alias("has_upgrade"),
    )
    .orderBy(F.col("opened_at_year_avg_closed_at_hour").desc())
)
