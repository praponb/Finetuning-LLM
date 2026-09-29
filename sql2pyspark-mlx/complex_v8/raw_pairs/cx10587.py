from pyspark.sql import functions as F

cleaned_policys = (
    df_policies
    .filter(F.col("start_date") >= F.lit("2024-05-07").cast("date"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        F.abs(F.col("premium") - 3521.8).alias("premium_distance"),
    )
)

combined_details = (
    cleaned_policys.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.segment").isin("VIP", "RETAIL")), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.end_date",
        F.col("p.segment").alias("holder_segment"),
        "p.holder_name",
    )
)

filtered = (
    combined_details.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.premium") < 3817.5), "left_anti")
    .filter(F.col("holder_segment") != "VIP")
)

enriched = (
    filtered.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.end_date",
        "s.holder_segment",
        "s.holder_name",
        F.col("p.region").alias("holder_region"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count("*").alias("claim_count"),
        F.max("reported_at").alias("last_claim_reported_at"),
    )
)

with_stats = (
    enriched.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        "c.last_claim_reported_at",
    )
)

result = (
    with_stats
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "holder_segment",
        "holder_name",
        "holder_region",
        "claim_count",
        "last_claim_reported_at",
        F.coalesce(F.col("claim_count"), F.lit(0)).alias("claim_count_filled"),
        F.lower(F.regexp_replace(F.trim(F.col("holder_name")), " ", "_")).alias("holder_name_slug"),
    )
    .orderBy(F.col("claim_count_filled").desc_nulls_first())
)
