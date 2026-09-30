from pyspark.sql import functions as F

matched = (
    df_claims.alias("c")
    .join(
        df_policies.alias("p"),
        (F.col("p.id") == F.col("c.policy_id"))
        & F.col("c.claim_date").between(F.col("p.start_date"), F.coalesce(F.col("p.end_date"), F.lit("2099-12-31").cast("date"))),
        "inner",
    )
    .select(
        "c.id",
        "c.policy_id",
        "c.claim_date",
        "c.claimed_amount",
        "c.status",
        "c.approved_amount",
        "p.product",
        "p.premium",
    )
)

annotated = (
    matched
    .withColumn("approved_amount_minus_claimed_amount", F.col("approved_amount") - F.col("claimed_amount"))
)

result = (
    annotated
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claimed_amount",
        "status",
        "approved_amount",
        "product",
        "premium",
        "approved_amount_minus_claimed_amount",
        F.when(F.col("claimed_amount") >= 33690.0, "A").when(F.col("claimed_amount") >= 13635.1, "B").otherwise("C").alias("claimed_amount_tier"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("claim_date")).alias("days_since_claim_date"),
    )
)
