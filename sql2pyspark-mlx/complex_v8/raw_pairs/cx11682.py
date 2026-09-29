from pyspark.sql import functions as F

scoped_policys = (
    df_policies
    .filter((F.col("holder_id").isNotNull()) & (F.col("status") == "LAPSED"))
    .select(
        "id",
        "holder_id",
        "premium",
        "end_date",
        "status",
    )
)

enriched = (
    scoped_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.end_date",
        "s.status",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

enriched_holder = (
    enriched.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.col("p.risk_score") > 0.3)
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.status",
        "s.holder_segment",
        "s.holder_joined_on",
        "s.holder_risk_score",
        F.col("p.region").alias("holder_region"),
        F.col("p.risk_score").alias("holder_risk_score_2"),
    )
)

labeled = (
    enriched_holder
    .filter(F.col("holder_joined_on").between(F.lit("2024-03-22").cast("date"), F.lit("2025-05-01").cast("date")))
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("holder_region")), " ", "_")).alias("holder_region_slug"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.count(F.when(~F.col("product").isin("HEALTH", "TRAVEL"), 1)).alias("policy_product_count"),
        F.min("end_date").alias("first_policy_end_date"),
        F.count(F.when(F.col("product") == "LIFE", 1)).alias("policy_product_count_2"),
    )
)

with_counts = (
    labeled.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.policy_product_count"), F.lit(0)).alias("policy_product_count"),
        "p.first_policy_end_date",
        F.coalesce(F.col("p.policy_product_count_2"), F.lit(0)).alias("policy_product_count_2"),
    )
)

enriched_holder_2 = (
    with_counts.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.col("p.joined_on") >= F.lit("2024-07-18").cast("date"))
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.holder_segment",
        "s.holder_risk_score",
        "s.holder_region",
        "s.holder_region_slug",
        "s.policy_product_count",
        "s.first_policy_end_date",
        F.col("p.joined_on").alias("holder_joined_on_2"),
    )
)

result = (
    enriched_holder_2
    .filter(F.col("premium") >= 2096.6)
    .select(
        "id",
        "holder_segment",
        "holder_risk_score",
        "holder_region_slug",
        "policy_product_count",
        "first_policy_end_date",
        "holder_joined_on_2",
    )
    .orderBy(F.col("policy_product_count").desc())
)
