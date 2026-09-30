from pyspark.sql import functions as F

valid_policys = (
    df_policies
    .filter((F.col("premium") < 2844.1) & (F.col("product").isin("HOME", "HEALTH", "LIFE")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
    )
)

line_level_claims = (
    valid_policys.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        F.col("c.id").alias("claim_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
    )
)

screened = (
    line_level_claims.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.approved_amount") >= 0.0) & (F.col("c.claimed_amount").between(7204.1, 34763.4)), "left_anti")
    .filter(F.col("claim_id") >= 66)
)

result = (
    screened
    .groupBy("claim_reported_at")
    .pivot("product", ["AUTO", "HEALTH", "TRAVEL"])
    .agg(F.max("claim_approved_amount"))
    .orderBy("claim_reported_at")
)
