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

policy_id_stats = (
    in_window
    .groupBy("policy_id")
    .agg(F.avg("prior_rows_30d").alias("avg_prior_rows_30d"))
)

outliers = (
    in_window.alias("s")
    .join(policy_id_stats.alias("g"), F.col("g.policy_id") == F.col("s.policy_id"), "inner")
    .filter(F.col("s.prior_rows_30d") > F.col("g.avg_prior_rows_30d"))
    .select(
        "s.*",
        "g.avg_prior_rows_30d",
        F.round(F.col("s.prior_rows_30d") - F.col("g.avg_prior_rows_30d"), 2).alias("prior_rows_30d_minus_avg"),
    )
)

enriched = (
    outliers.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.prior_rows_30d",
        "s.prior_claimed_amount_30d",
        "s.avg_prior_rows_30d",
        "s.prior_rows_30d_minus_avg",
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.end_date").alias("policy_end_date"),
        F.col("po.risk_score").alias("holder_risk_score"),
    )
)

result = (
    enriched
    .filter(F.col("policy_id").isNotNull())
    .select(
        "id",
        "policy_id",
        "claim_date",
        "prior_rows_30d",
        "avg_prior_rows_30d",
        "prior_rows_30d_minus_avg",
        "policy_holder_id",
        "policy_end_date",
        "holder_risk_score",
        F.trunc(F.col("claim_date"), "MM").alias("claim_date_month_start"),
    )
    .orderBy(F.col("prior_rows_30d_minus_avg").asc())
)
