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
        "c.adjuster_id",
        "p.product",
        "p.premium",
    )
)

detail_rows_claims = (
    matched.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.status",
        "s.adjuster_id",
        "s.product",
        F.col("c.id").alias("claim_id"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
    )
)

detail_rows_claims_2 = (
    detail_rows_claims.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.year(F.col("c.claim_date")) == 2024), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.product",
        "s.claim_id",
        "s.claim_adjuster_id",
        "s.claim_approved_amount",
        F.col("c.id").alias("claim_id_2"),
        F.col("c.status").alias("claim_status"),
        F.col("c.approved_amount").alias("claim_approved_amount_2"),
        F.col("c.claimed_amount").alias("claim_claimed_amount_2"),
        "c.claim_date",
    )
)

with_activity = (
    detail_rows_claims_2.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left_semi")
)

result = (
    with_activity
    .filter(F.col("claim_date") < F.lit("2024-09-08").cast("date"))
    .select(
        "id",
        "adjuster_id",
        "product",
        "claim_id",
        "claim_adjuster_id",
        "claim_approved_amount",
        "claim_id_2",
        "claim_status",
        "claim_approved_amount_2",
        "claim_claimed_amount_2",
        "claim_date",
    )
)
