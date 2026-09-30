from pyspark.sql import functions as F

cleaned = (
    df_policies
    .filter(F.col("product") != "HOME")
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.month(F.col("start_date")).alias("start_date_month"),
        F.length(F.trim(F.col("product"))).alias("product_len"),
        F.substring(F.trim(F.col("product")), 1, 4).alias("product_prefix"),
    )
)

with_children = (
    cleaned.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.start_date",
        "s.product_prefix",
        F.col("c.id").alias("claim_id"),
        "c.claim_type",
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        "c.claim_date",
    )
)

joined = (
    with_children.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.risk_score") >= 0.2), "inner")
    .select(
        "s.*",
        F.col("p.region").alias("holder_region"),
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

result = (
    joined
    .groupingSets(
        [["product", "holder_region", "claim_type"], ["product"], []],
        "product", "holder_region", "claim_type",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.min("start_date").alias("first_start_date"),
        F.min("claim_approved_amount").alias("min_claim_approved_amount"),
    )
)
