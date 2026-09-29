from pyspark.sql import functions as F

recent = (
    df_policyholders
    .filter(F.col("risk_score").between(0.1, 0.6))
    .select(
        "id",
        "holder_name",
        "risk_score",
        "joined_on",
        F.concat(F.col("holder_name"), F.lit("#"), F.col("id").cast("string")).alias("holder_name_ref"),
        F.abs(F.col("risk_score") - 0.6).alias("risk_score_distance"),
        F.length(F.trim(F.col("region"))).alias("region_len"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.count("*").alias("policy_count"),
        F.count(F.when(F.col("product") == "HEALTH", 1)).alias("policy_product_count"),
    )
)

summarized_rows = (
    recent.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.policy_count"), F.lit(0)).alias("policy_count"),
        F.coalesce(F.col("p.policy_product_count"), F.lit(0)).alias("policy_product_count"),
    )
)

scored = (
    summarized_rows
    .filter(F.col("policy_product_count") >= 4)
    .select(
        "*",
        F.round(F.col("risk_score_distance") * 0.25, 2).alias("risk_score_distance_adj"),
        F.greatest(F.col("risk_score_distance"), F.col("policy_product_count")).alias("greatest_risk_score_distance_policy_product_coun"),
        F.upper(F.trim(F.col("holder_name_ref"))).alias("holder_name_ref_clean"),
    )
)

enriched = (
    scored
    .filter(F.col("risk_score_distance") < 0.7)
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("holder_name_ref")), " ", "_")).alias("holder_name_ref_slug"),
        F.date_trunc("week", F.col("joined_on")).alias("joined_on_week"),
    )
)

result = (
    enriched
    .filter(F.col("holder_name_ref").isNotNull())
    .select(
        "id",
        "holder_name",
        "risk_score",
        "holder_name_ref",
        "risk_score_distance",
        "policy_count",
        "risk_score_distance_adj",
        "greatest_risk_score_distance_policy_product_coun",
        "holder_name_ref_clean",
        "holder_name_ref_slug",
        "joined_on_week",
        F.lower(F.regexp_replace(F.trim(F.col("holder_name_ref")), " ", "_")).alias("holder_name_ref_slug_2"),
    )
    .orderBy("risk_score_distance_adj", "id")
    .limit(20)
)
