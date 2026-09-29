from pyspark.sql import functions as F

scoped = (
    df_policies
    .filter(~F.col("product").isin("AUTO", "LIFE"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.datediff(F.col("end_date"), F.col("start_date")).alias("days_start_date_to_end_date"),
        F.round(F.col("premium") * 1.5, 2).alias("premium_adj"),
        F.substring(F.trim(F.col("product")), 1, 3).alias("product_prefix"),
    )
)

with_parent = (
    scoped.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.end_date",
        "s.status",
        "s.days_start_date_to_end_date",
        "s.product_prefix",
        "p.holder_name",
    )
)

line_level = (
    with_parent.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        "s.status",
        F.col("c.id").alias("claim_id"),
        F.col("c.status").alias("claim_status"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        "c.claim_type",
        "c.claim_date",
    )
)

result = (
    line_level
    .filter(F.col("claim_id").isNotNull())
    .select(
        "id",
        "holder_id",
        "start_date",
        "status",
        "claim_id",
        "claim_claimed_amount",
        F.when(F.col("claim_claimed_amount") >= 34763.4, "A").when(F.col("claim_claimed_amount") >= 18364.1, "B").otherwise("C").alias("claim_claimed_amount_tier"),
    )
    .orderBy(F.col("id").desc(), "claim_claimed_amount_tier")
)
