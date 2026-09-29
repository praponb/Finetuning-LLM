from pyspark.sql import functions as F

cleaned_policys = (
    df_policies
    .filter(F.col("start_date") >= F.lit("2024-10-22").cast("date"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
        F.substring(F.trim(F.col("product")), 1, 4).alias("product_prefix"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count("*").alias("claim_count"),
        F.max("approved_amount").alias("max_claim_approved_amount"),
        F.sum("claimed_amount").alias("total_claim_claimed_amount"),
    )
)

profiled = (
    cleaned_policys.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        "c.max_claim_approved_amount",
        "c.total_claim_claimed_amount",
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(F.count("*").alias("policy_count"))
)

summarized_rows = (
    profiled.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.policy_count"), F.lit(0)).alias("policy_count"),
    )
)

joined = (
    summarized_rows.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.holder_name").isNotNull()), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.status",
        "s.product_prefix",
        "s.claim_count",
        "s.total_claim_claimed_amount",
        "s.policy_count",
        F.col("p.segment").alias("holder_segment"),
    )
)

result = (
    joined
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
        "product_prefix",
        "total_claim_claimed_amount",
        "policy_count",
        "holder_segment",
        F.substring(F.trim(F.col("status")), 1, 3).alias("status_prefix"),
        F.concat_ws("-", F.col("product_prefix"), F.col("holder_segment")).alias("product_prefix_holder_segment_label"),
    )
    .orderBy(F.col("start_date").desc())
)
