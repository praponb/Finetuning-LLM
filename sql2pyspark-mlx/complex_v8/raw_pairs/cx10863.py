from pyspark.sql import functions as F

prepared = (
    df_policyholders
    .filter((F.col("segment") == "VIP") & (F.col("risk_score") >= 0.2))
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on",
        F.concat(F.col("holder_name"), F.lit("#"), F.col("id").cast("string")).alias("holder_name_ref"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(F.count("*").alias("policy_count"))
)

summarized_rows = (
    prepared.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.policy_count"), F.lit(0)).alias("policy_count"),
    )
)

scored = (
    summarized_rows
    .filter(F.col("risk_score") <= 0.6)
    .select(
        "*",
        F.substring(F.trim(F.col("region")), 1, 2).alias("region_prefix"),
        F.month(F.col("joined_on")).alias("joined_on_month"),
    )
)

enriched = (
    scored
    .filter(F.col("policy_count") < 7)
    .select(
        "*",
        F.round(F.col("risk_score") * 0.9, 2).alias("risk_score_adj"),
        F.round(F.col("risk_score") * 1.1, 2).alias("risk_score_adj_2"),
        F.date_format(F.col("joined_on"), "yyyy-MM-dd").alias("joined_on_label"),
    )
)

region_stats = (
    enriched
    .groupBy("region")
    .agg(F.avg("policy_count").alias("avg_policy_count"))
)

compared = (
    enriched.alias("s")
    .join(region_stats.alias("g"), F.col("g.region") == F.col("s.region"), "inner")
    .filter(F.col("s.policy_count") > F.col("g.avg_policy_count"))
    .select(
        "s.*",
        "g.avg_policy_count",
        F.round(F.col("s.policy_count") - F.col("g.avg_policy_count"), 2).alias("policy_count_minus_avg"),
    )
)

result = (
    compared
    .groupBy("policy_count")
    .pivot("region", ["South", "North", "East"])
    .agg(F.max("avg_policy_count"))
)
