from pyspark.sql import functions as F

covered = (
    df_claims.alias("c")
    .join(
        df_policies.alias("p"),
        (F.col("p.id") == F.col("c.policy_id"))
        & F.col("c.claim_date").between(F.col("p.start_date"), F.coalesce(F.col("p.end_date"), F.lit("2099-12-31").cast("date"))),
        "left",
    )
    .select(
        "c.id",
        "c.policy_id",
        "c.claim_date",
        "c.claimed_amount",
        "c.adjuster_id",
        "c.claim_type",
        "p.product",
        "p.premium",
    )
)

enriched = (
    covered
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claimed_amount",
        "claim_type",
        "product",
        "premium",
        F.least(F.col("premium"), F.col("claimed_amount")).alias("least_premium_claimed_amount"),
        F.greatest(F.col("claimed_amount"), F.col("premium")).alias("greatest_claimed_amount_premium"),
    )
)

combined_details = (
    enriched.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .filter(F.col("p.status") == "CANCELLED")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.claim_type",
        "s.product",
        "s.premium",
        "s.least_premium_claimed_amount",
        F.col("p.product").alias("policy_product"),
        F.col("p.status").alias("policy_status"),
        F.col("p.start_date").alias("policy_start_date"),
    )
)

result = (
    combined_details
    .select(
        "id",
        "claim_date",
        "claim_type",
        "product",
        "least_premium_claimed_amount",
        "policy_product",
        "policy_status",
        F.round(F.col("premium") * 0.25, 2).alias("premium_adj"),
        F.add_months(F.col("policy_start_date"), 3).alias("policy_start_date_plus_3m"),
    )
    .orderBy(F.col("premium_adj").desc())
)
