from pyspark.sql import functions as F

covered = (
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

scored = (
    covered
    .select(
        "*",
        F.abs(F.col("prior_claimed_amount_30d") - 87500.0).alias("prior_claimed_amount_30d_distance"),
        (F.floor(F.col("prior_claimed_amount_30d") / 50000) * 50000).alias("prior_claimed_amount_30d_bucket"),
        F.round(F.col("prior_claimed_amount_30d") * 1.5, 2).alias("prior_claimed_amount_30d_adj"),
    )
)

combined_details = (
    scored.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.prior_rows_30d",
        "s.prior_claimed_amount_30d_distance",
        "s.prior_claimed_amount_30d_bucket",
        F.col("p.start_date").alias("policy_start_date"),
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.premium").alias("policy_premium"),
        F.col("po.segment").alias("holder_segment"),
    )
)

filtered = (
    combined_details.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.status").isin("EXPIRED", "LAPSED")), "left_semi")
)

result = (
    filtered
    .select(
        "id",
        "policy_id",
        "prior_rows_30d",
        "prior_claimed_amount_30d_distance",
        "prior_claimed_amount_30d_bucket",
        "policy_end_date",
        "policy_premium",
        "holder_segment",
        (F.col("prior_rows_30d") + F.col("policy_premium")).alias("prior_rows_30d_plus_policy_premium"),
    )
    .orderBy("id")
)
