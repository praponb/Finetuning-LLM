from pyspark.sql import functions as F

covered = (
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
        "c.claim_type",
        "c.adjuster_id",
        "p.product",
        "p.premium",
    )
)

rollup_base = (
    covered
    .groupBy(F.year("claim_date").alias("claim_date_year"), "policy_id")
    .agg(
        F.round(F.avg("claimed_amount"), 2).alias("avg_claimed_amount"),
        F.round(F.avg("premium"), 2).alias("avg_premium"),
        F.min("claim_date").alias("first_claim_date"),
        F.sum(F.when(F.col("product").isin("LIFE", "HEALTH"), F.col("claimed_amount"))).alias("claimed_amount_product_in_life_health"),
        F.datediff(F.max("claim_date"), F.min("claim_date")).alias("claim_date_span_days"),
    )
)

with_parent = (
    rollup_base.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.claim_date_year",
        "s.policy_id",
        "s.first_claim_date",
        "s.claimed_amount_product_in_life_health",
        F.col("p.status").alias("policy_status"),
    )
)

result = (
    with_parent
    .filter(F.col("claimed_amount_product_in_life_health") < 187500.0)
    .select(
        "claim_date_year",
        "policy_id",
        "first_claim_date",
        "claimed_amount_product_in_life_health",
        "policy_status",
        F.coalesce(F.col("policy_status"), F.lit("UNKNOWN")).alias("policy_status_or_unknown"),
        F.lower(F.regexp_replace(F.trim(F.col("policy_status")), " ", "_")).alias("policy_status_slug"),
    )
    .orderBy(
        F.col("policy_status_or_unknown").asc(),
        F.col("first_claim_date").desc(),
        "claim_date_year",
        "policy_id",
    )
    .limit(5)
)
