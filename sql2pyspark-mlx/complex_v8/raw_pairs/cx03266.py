from pyspark.sql import functions as F

joined_ranges = (
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
        "c.claim_type",
        "c.status",
        "p.product",
        "p.premium",
    )
)

derived = (
    joined_ranges
    .filter(F.col("premium") < 3465.4)
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claimed_amount",
        "claim_type",
        "status",
        "product",
        F.add_months(F.col("claim_date"), 6).alias("claim_date_plus_6m"),
    )
)

result = (
    derived
    .filter(F.col("product") == "LIFE")
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claim_type",
        "status",
        "product",
        "claim_date_plus_6m",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("claim_date")).alias("days_since_claim_date"),
        F.round(F.col("claimed_amount") * 0.9, 2).alias("claimed_amount_adj"),
    )
)
