from pyspark.sql import functions as F

matched = (
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
        "c.status",
        "c.claim_type",
        "p.product",
        "p.premium",
    )
)

with_parent_policy = (
    matched.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .filter(F.year(F.col("p.start_date")) == 2024)
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.status",
        "s.claim_type",
        "s.product",
        "s.premium",
        F.col("p.start_date").alias("policy_start_date"),
        F.col("p.end_date").alias("policy_end_date"),
    )
)

labeled = (
    with_parent_policy
    .filter(F.col("product") != "AUTO")
    .select(
        "*",
        F.least(F.col("premium"), F.col("claimed_amount")).alias("least_premium_claimed_amount"),
        F.datediff(F.col("policy_end_date"), F.col("policy_start_date")).alias("days_policy_start_date_to_policy_end_date"),
    )
)

result = (
    labeled
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claimed_amount",
        "status",
        "claim_type",
        "product",
        "premium",
        "least_premium_claimed_amount",
        "days_policy_start_date_to_policy_end_date",
        F.date_trunc("quarter", F.col("policy_start_date")).alias("policy_start_date_quarter"),
        F.round(F.col("claimed_amount") / F.nullif(F.col("premium"), F.lit(0)), 3).alias("claimed_amount_per_premium"),
    )
    .orderBy(F.col("policy_id").desc(), "id")
    .limit(10)
)
