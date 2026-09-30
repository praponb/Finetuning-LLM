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

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.max("claim_date").alias("last_claim_date"),
        F.count("*").alias("claim_count"),
    )
)

with_counts = (
    covered.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        "c.last_claim_date",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

summary = (
    with_counts
    .filter(F.col("claim_count") > 4)
    .groupBy("policy_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.round(F.stddev("prior_rows_30d"), 2).alias("stddev_prior_rows_30d"),
    )
)

result = (
    summary
    .select(
        "policy_id",
        "n_rows",
        (F.col("stddev_prior_rows_30d") + F.col("n_rows")).alias("stddev_prior_rows_30d_plus_n_rows"),
        (F.floor(F.col("n_rows") / 2) * 2).alias("n_rows_bucket"),
    )
    .orderBy(
        F.col("n_rows").desc_nulls_first(),
        F.col("stddev_prior_rows_30d_plus_n_rows").desc_nulls_first(),
        "policy_id",
    )
    .limit(20)
)
