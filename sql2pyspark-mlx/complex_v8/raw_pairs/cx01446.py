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

with_parent = (
    covered.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.adjuster_id",
        "s.claim_type",
        "s.product",
        F.col("a.team").alias("adjuster_team"),
        "a.adjuster_name",
    )
)

result = (
    with_parent
    .filter(F.col("adjuster_id") <= 4)
    .select(
        "id",
        "policy_id",
        "claimed_amount",
        "claim_type",
        "product",
        "adjuster_team",
        "adjuster_name",
        F.coalesce(F.col("claimed_amount"), F.lit(0.0)).alias("claimed_amount_filled"),
    )
    .orderBy("adjuster_name", "id")
    .limit(20)
)
