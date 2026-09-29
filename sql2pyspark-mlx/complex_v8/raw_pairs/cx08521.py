from pyspark.sql import functions as F

scoped_policys = (
    df_policies
    .filter(F.col("premium") >= 2096.6)
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
        F.when(F.col("premium") >= 3465.4, "HIGH").when(F.col("premium") >= 1541.8, "MEDIUM").otherwise("LOW").alias("premium_tier"),
    )
)

line_level = (
    scoped_policys.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.start_date",
        "s.end_date",
        "s.premium_filled",
        F.col("c.id").alias("claim_id"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.status").alias("claim_status"),
    )
)

result = (
    line_level
    .groupingSets(
        [["claim_status", "product"], ["claim_status"]],
        "claim_status", "product",
    )
    .agg(
        F.grouping("claim_status").alias("is_total_claim_status"),
        F.round(100 * F.count(F.when(F.col("claim_status") == "OPEN", 1)) / F.count("*"), 1).alias("pct_claim_status_open"),
        F.sum(F.when(F.col("claim_status").isin("APPROVED", "OPEN"), F.col("claim_approved_amount"))).alias("claim_approved_amount_claim_status_in_approved_o"),
    )
    .select(
        F.coalesce(F.col("claim_status"), F.lit("ALL")).alias("claim_status"),
        F.coalesce(F.col("product"), F.lit("ALL")).alias("product"),
        "is_total_claim_status",
        "pct_claim_status_open",
        "claim_approved_amount_claim_status_in_approved_o",
    )
    .orderBy(
        F.col("claim_status").asc_nulls_last(),
        F.col("product").asc_nulls_last(),
    )
)
