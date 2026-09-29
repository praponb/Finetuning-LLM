from pyspark.sql import functions as F

joined_ranges = (
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
        "c.approved_amount",
        "c.claim_type",
        "p.product",
        "p.premium",
    )
)

annotated = (
    joined_ranges
    .filter(F.col("approved_amount") <= 32878.5)
    .select(
        "id",
        "policy_id",
        "approved_amount",
        "claim_type",
        "product",
        "premium",
        F.length(F.trim(F.col("claim_type"))).alias("claim_type_len"),
        F.round(F.col("claimed_amount") / F.nullif(F.col("premium"), F.lit(0)), 3).alias("claimed_amount_per_premium"),
    )
)

result = (
    annotated
    .select(
        "id",
        F.col("premium").alias("m_premium"),
        F.col("claimed_amount_per_premium").alias("m_claimed_amount_per_premium"),
    )
    .unpivot(
        ids=["id"],
        values=["m_premium", "m_claimed_amount_per_premium"],
        variableColumnName="field",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
