from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_cnt_agent_tier = Window.partitionBy("cnt", "agent_tier")
w_n_agent_team_onboarding_cnt = Window.partitionBy("n_agent_team_onboarding", "cnt").orderBy(F.col("avg_csat").desc())

scoped_tickets = (
    df_tickets
    .filter((F.col("agent_id").isNotNull()) | (F.size(F.col("tags")) >= 1))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.concat_ws("|", F.col("channel"), F.col("priority")).alias("channel_priority_label"),
    )
)

enriched = (
    scoped_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.team").alias("agent_team"),
        F.col("su.tier").alias("agent_tier"),
    )
)

with_details = (
    enriched.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.channel",
        "s.csat",
        "s.tags",
        "s.channel_priority_label",
        "s.agent_hired_on",
        "s.agent_team",
        "s.agent_tier",
        F.col("su.team").alias("agent_team_2"),
        F.col("su.tier").alias("agent_tier_2"),
        F.col("su.hired_on").alias("agent_hired_on_2"),
    )
)

rollup_base = (
    with_details
    .filter(F.col("agent_team") == "Tech")
    .groupBy(
        "channel",
        "agent_tier",
        F.date_trunc("month", "agent_hired_on").alias("agent_hired_on_month"),
    )
    .agg(
        F.sum(F.when(F.col("agent_team") != "Onboarding", 1).otherwise(0)).alias("n_agent_team_onboarding"),
        F.round(F.avg("csat"), 2).alias("avg_csat"),
        F.count("*").alias("cnt"),
        F.countDistinct("agent_tier").alias("distinct_agent_tiers"),
        F.sum("csat").alias("total_csat"),
    )
)

filtered = (
    rollup_base
    .crossJoin(rollup_base.agg(F.avg("avg_csat").alias("overall_avg_avg_csat")))
    .filter(F.col("avg_csat") > F.col("overall_avg_avg_csat"))
    .drop("overall_avg_avg_csat")
)

metrics = (
    filtered
    .withColumn("rows_in_cnt_agent_tier", F.count(F.lit(1)).over(w_cnt_agent_tier))
    .withColumn("avg_csat_dense_rank", F.dense_rank().over(w_n_agent_team_onboarding_cnt))
)

result = (
    metrics
    .select(
        "channel",
        "agent_hired_on_month",
        "n_agent_team_onboarding",
        "cnt",
        "distinct_agent_tiers",
        "total_csat",
        "rows_in_cnt_agent_tier",
        "avg_csat_dense_rank",
        F.substring(F.trim(F.col("agent_tier")), 1, 4).alias("agent_tier_prefix"),
    )
    .orderBy(F.col("n_agent_team_onboarding").desc())
)
