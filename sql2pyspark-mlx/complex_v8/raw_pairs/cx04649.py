from pyspark.sql import functions as F

cleaned_policys = (
    df_policies
    .filter((F.col("product") == "HOME") & (F.col("start_date") < F.lit("2025-03-06").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        F.coalesce(F.col("product"), F.lit("UNKNOWN")).alias("product_or_unknown"),
        F.when(F.col("status").isin("ACTIVE", "EXPIRED"), "group_a").when(F.col("status") == "CANCELLED", "group_b").otherwise("other").alias("status_group"),
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
    )
)

qualified = (
    cleaned_policys.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.adjuster_id").isNotNull()) & (F.col("c.status").isNotNull()), "left_anti")
)

with_parent = (
    qualified.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.status_group",
        "s.premium_filled",
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.region").alias("holder_region"),
    )
)

detail_rows = (
    with_parent.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.claimed_amount") < 41878.5), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.holder_risk_score",
        "s.holder_joined_on",
        "s.holder_region",
        F.col("c.id").alias("claim_id"),
        "c.claim_type",
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
    )
)

result = (
    detail_rows
    .groupBy("product")
    .pivot("holder_region", ["North", "Central"])
    .agg(F.sum("claim_claimed_amount").alias("total"), F.count("claim_claimed_amount").alias("n"))
    .orderBy("product")
)
