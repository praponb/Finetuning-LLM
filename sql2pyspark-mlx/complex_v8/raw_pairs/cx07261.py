from pyspark.sql import functions as F

matched = (
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
        F.sum("b.approved_amount").alias("prior_approved_amount_30d"),
    )
)

policy_id_stats = (
    matched
    .groupBy("policy_id")
    .agg(F.avg("prior_rows_30d").alias("avg_prior_rows_30d"))
)

compared = (
    matched.alias("s")
    .join(policy_id_stats.alias("g"), F.col("g.policy_id") == F.col("s.policy_id"), "inner")
    .filter(F.col("s.prior_rows_30d") > F.col("g.avg_prior_rows_30d"))
    .select(
        "s.*",
        "g.avg_prior_rows_30d",
        F.round(F.col("s.prior_rows_30d") - F.col("g.avg_prior_rows_30d"), 2).alias("prior_rows_30d_minus_avg"),
    )
)

result = (
    compared
    .select(
        "id",
        "policy_id",
        "claim_date",
        "prior_rows_30d",
        "prior_approved_amount_30d",
        F.round(F.col("avg_prior_rows_30d") / F.nullif(F.col("prior_rows_30d"), F.lit(0)), 3).alias("avg_prior_rows_30d_per_prior_rows_30d"),
        F.date_add(F.col("claim_date"), 30).alias("claim_date_plus_30d"),
    )
)
