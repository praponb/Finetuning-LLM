from pyspark.sql import functions as F

in_window = (
    df_claims.alias("a")
    .join(
        df_claims.alias("b"),
        (F.col("b.policy_id") == F.col("a.policy_id"))
        & (F.col("b.claim_date") < F.col("a.claim_date"))
        & (F.col("b.claim_date") >= F.date_sub(F.col("a.claim_date"), 180)),
        "left",
    )
    .groupBy("a.id", "a.policy_id", "a.claim_date")
    .agg(
        F.count("b.id").alias("prior_rows_180d"),
        F.sum("b.claimed_amount").alias("prior_claimed_amount_180d"),
    )
)

screened = (
    in_window.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left_semi")
    .filter(F.col("prior_rows_180d") > 3)
)

joined = (
    screened.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.prior_claimed_amount_180d",
        F.col("p.status").alias("policy_status"),
        F.col("po.joined_on").alias("holder_joined_on"),
        F.col("po.risk_score").alias("holder_risk_score"),
    )
)

result = (
    joined
    .filter(F.col("holder_risk_score") >= 0.1)
    .select(
        "id",
        "policy_id",
        "claim_date",
        "policy_status",
        "holder_risk_score",
        F.coalesce(F.col("holder_risk_score"), F.lit(0.0)).alias("holder_risk_score_filled"),
        F.substring(F.trim(F.col("policy_status")), 1, 2).alias("policy_status_prefix"),
    )
    .orderBy(F.col("holder_risk_score").desc())
)
