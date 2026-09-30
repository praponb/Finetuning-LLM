from pyspark.sql import functions as F

prepared_claims = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "status",
        "reported_at",
        F.date_trunc("month", F.col("reported_at")).alias("reported_at_month"),
    )
)

status_stats = (
    prepared_claims
    .groupBy("status")
    .agg(F.avg("claimed_amount").alias("avg_claimed_amount"))
)

above_average = (
    prepared_claims.alias("s")
    .join(status_stats.alias("g"), F.col("g.status") == F.col("s.status"), "inner")
    .filter(F.col("s.claimed_amount") > F.col("g.avg_claimed_amount"))
    .select(
        "s.*",
        "g.avg_claimed_amount",
        F.round(F.col("s.claimed_amount") - F.col("g.avg_claimed_amount"), 2).alias("claimed_amount_minus_avg"),
    )
)

without_matches = (
    above_average.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.approved_amount") <= 17759.3) & (F.col("c.claim_date").isNotNull()), "left_anti")
    .filter(F.col("policy_id") <= 14)
)

result = (
    without_matches
    .cube("status", "policy_id")
    .agg(
        F.grouping_id().alias("gid"),
        F.count(F.when(F.col("status").isNotNull(), 1)).alias("n_status_is_not_null"),
    )
)
