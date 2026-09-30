from pyspark.sql import functions as F

cleaned_policys = (
    df_policies
    .filter((F.col("product").isNotNull()) & (F.col("end_date") < F.lit("2026-09-19").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.round(F.col("premium") * 1.5, 2).alias("premium_adj"),
        F.length(F.trim(F.col("status"))).alias("status_len"),
        F.year(F.col("end_date")).alias("end_date_year"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count("*").alias("claim_count"),
        F.count(F.when(F.col("status") == "IN_REVIEW", 1)).alias("claim_status_count"),
        F.count("*").alias("claim_count_2"),
    )
)

with_counts = (
    cleaned_policys.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        F.coalesce(F.col("c.claim_status_count"), F.lit(0)).alias("claim_status_count"),
        F.coalesce(F.col("c.claim_count_2"), F.lit(0)).alias("claim_count_2"),
    )
)

enriched = (
    with_counts.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.end_date",
        "s.status",
        "s.status_len",
        "s.claim_count",
        "s.claim_status_count",
        "s.claim_count_2",
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

without_matches = (
    enriched
    .crossJoin(enriched.agg(F.avg("claim_count_2").alias("overall_avg_claim_count_2")))
    .filter(F.col("claim_count_2") > F.col("overall_avg_claim_count_2"))
    .drop("overall_avg_claim_count_2")
)

result = (
    without_matches
    .rollup("claim_count", "status", "product")
    .agg(
        F.grouping_id().alias("gid"),
        F.max("claim_status_count").alias("max_claim_status_count"),
        F.round(F.stddev("status_len"), 2).alias("stddev_status_len"),
    )
    .select(
        "claim_count",
        F.coalesce(F.col("status"), F.lit("ALL")).alias("status"),
        F.coalesce(F.col("product"), F.lit("ALL")).alias("product"),
        "gid",
        "max_claim_status_count",
        "stddev_status_len",
    )
    .orderBy(
        F.col("claim_count").asc_nulls_last(),
        F.col("status").asc_nulls_last(),
        F.col("product").asc_nulls_last(),
    )
)
