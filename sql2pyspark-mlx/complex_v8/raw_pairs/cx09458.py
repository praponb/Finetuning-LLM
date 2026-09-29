from pyspark.sql import functions as F

in_window = (
    df_claims.alias("a")
    .join(
        df_claims.alias("b"),
        (F.col("b.policy_id") == F.col("a.policy_id"))
        & (F.col("b.claim_date") < F.col("a.claim_date"))
        & (F.col("b.claim_date") >= F.date_sub(F.col("a.claim_date"), 30)),
        "left",
    )
    .groupBy("a.id", "a.policy_id", "a.claim_date")
    .agg(
        F.count("b.id").alias("prior_rows_30d"),
        F.sum("b.claimed_amount").alias("prior_claimed_amount_30d"),
    )
)

with_activity = (
    in_window.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.premium") <= 4678.8), "left_anti")
)

screened = (
    with_activity.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.claim_date") < F.lit("2025-03-31").cast("date")), "left_semi")
    .filter(F.col("prior_claimed_amount_30d") >= 100000.0)
)

result = (
    screened
    .select(
        "id",
        "policy_id",
        "claim_date",
        "prior_claimed_amount_30d",
    )
    .orderBy("id")
    .limit(20)
)
