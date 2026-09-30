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

policy_id_stats = (
    covered
    .groupBy("policy_id")
    .agg(F.max("prior_rows_30d").alias("max_prior_rows_30d"))
)

outliers = (
    covered.alias("s")
    .join(policy_id_stats.alias("g"), F.col("g.policy_id") == F.col("s.policy_id"), "inner")
    .filter(F.col("s.prior_rows_30d") == F.col("g.max_prior_rows_30d"))
    .select(
        "s.*",
        "g.max_prior_rows_30d",
        F.round(F.col("s.prior_rows_30d") - F.col("g.max_prior_rows_30d"), 2).alias("prior_rows_30d_minus_max"),
    )
)

detail_rows_claims = (
    outliers.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.prior_rows_30d",
        "s.prior_claimed_amount_30d",
        "s.max_prior_rows_30d",
        "s.prior_rows_30d_minus_max",
        F.col("c.id").alias("claim_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        "c.claim_type",
    )
)

result = (
    detail_rows_claims
    .select(
        "id",
        "claim_date",
        "prior_rows_30d",
        "prior_claimed_amount_30d",
        "max_prior_rows_30d",
        "prior_rows_30d_minus_max",
        "claim_id",
        F.greatest(F.col("prior_rows_30d_minus_max"), F.col("claim_approved_amount")).alias("greatest_prior_rows_30d_minus_max_claim_approved"),
        F.lower(F.regexp_replace(F.trim(F.col("claim_type")), " ", "_")).alias("claim_type_slug"),
    )
    .orderBy(
        F.col("claim_id").desc(),
        "greatest_prior_rows_30d_minus_max_claim_approved",
    )
)
