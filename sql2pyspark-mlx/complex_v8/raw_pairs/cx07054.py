from pyspark.sql import functions as F

cleaned_claims = (
    df_claims
    .filter((F.col("approved_amount") > 6817.6) & (F.col("reported_at") >= F.lit("2025-01-31 13:00:00").cast("timestamp")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        (F.col("reported_at") + F.expr("INTERVAL 2 HOURS")).alias("reported_at_plus_2h"),
        F.abs(F.col("approved_amount") - 17759.3).alias("approved_amount_distance"),
        F.date_trunc("month", F.col("reported_at")).alias("reported_at_month"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(F.count("*").alias("claim_count"))
)

summarized_rows = (
    cleaned_claims.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

result = (
    summarized_rows
    .groupingSets(
        [["adjuster_id", "claim_count"], ["adjuster_id"], []],
        "adjuster_id", "claim_count",
    )
    .agg(
        F.max("reported_at_plus_2h").alias("last_reported_at_plus_2h"),
        F.count(F.when(F.col("claim_type") == "fire", 1)).alias("n_claim_type_fire"),
    )
    .orderBy(
        F.col("adjuster_id").asc_nulls_last(),
        F.col("claim_count").asc_nulls_last(),
    )
)
