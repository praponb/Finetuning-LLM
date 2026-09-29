from pyspark.sql import functions as F

recent_claims = (
    df_claims
    .filter(F.col("claimed_amount") < 42447.2)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "status",
        "reported_at",
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.count("*").alias("claim_count"),
        F.count(F.when(F.col("status").isin("PAID", "REJECTED"), 1)).alias("claim_status_count"),
        F.count(F.when(F.col("status") != "APPROVED", 1)).alias("claim_status_count_2"),
    )
)

with_stats = (
    recent_claims.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        F.coalesce(F.col("c.claim_status_count"), F.lit(0)).alias("claim_status_count"),
        F.coalesce(F.col("c.claim_status_count_2"), F.lit(0)).alias("claim_status_count_2"),
    )
)

status_stats = (
    with_stats
    .groupBy("status")
    .agg(F.max("claim_status_count_2").alias("max_claim_status_count_2"))
)

compared = (
    with_stats.alias("s")
    .join(status_stats.alias("g"), F.col("g.status") == F.col("s.status"), "inner")
    .filter(F.col("s.claim_status_count_2") == F.col("g.max_claim_status_count_2"))
    .select(
        "s.*",
        "g.max_claim_status_count_2",
        F.round(F.col("s.claim_status_count_2") - F.col("g.max_claim_status_count_2"), 2).alias("claim_status_count_2_minus_max"),
    )
)

without_matches = (
    compared
    .crossJoin(compared.agg(F.max("max_claim_status_count_2").alias("overall_max_max_claim_status_count_2")))
    .filter(F.col("max_claim_status_count_2") < F.col("overall_max_max_claim_status_count_2"))
    .drop("overall_max_max_claim_status_count_2")
)

result = (
    without_matches
    .groupingSets(
        [["claim_status_count", "claim_count", "adjuster_id"], ["claim_status_count"], []],
        "claim_status_count", "claim_count", "adjuster_id",
    )
    .agg(
        F.grouping("claim_status_count").alias("is_total_claim_status_count"),
        F.sum("claim_status_count_2").alias("total_claim_status_count_2"),
    )
)
