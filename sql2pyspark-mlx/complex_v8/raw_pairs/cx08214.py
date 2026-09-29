from pyspark.sql import functions as F

in_window = (
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
        "c.adjuster_id",
        "p.product",
        "p.premium",
    )
)

enriched_policy = (
    in_window.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.status",
        "s.adjuster_id",
        "s.product",
        F.col("p.start_date").alias("policy_start_date"),
    )
)

result = (
    enriched_policy
    .filter(F.col("status") == "APPROVED")
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claimed_amount",
        "status",
        "adjuster_id",
        "policy_start_date",
        (F.floor(F.col("claimed_amount") / 10000) * 10000).alias("claimed_amount_bucket"),
    )
    .orderBy(F.col("claimed_amount_bucket").desc(), "id")
    .limit(20)
)
