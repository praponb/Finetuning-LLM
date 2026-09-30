from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("adjuster_id", "claim_type_or_unknown").orderBy(F.col("approved_amount").desc(), F.col("id").desc())
w_latest = Window.partitionBy("policy_id", "adjuster_id").orderBy(F.col("claim_claim_type_count").desc(), F.col("id").desc())

recent_claims = (
    df_claims
    .filter(F.col("approved_amount") >= 0.0)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        F.coalesce(F.col("claim_type"), F.lit("UNKNOWN")).alias("claim_type_or_unknown"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("reported_at")).alias("days_since_reported_at"),
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
    )
)

filtered = (
    recent_claims.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.policy_id").isNotNull()) & (F.col("c.claim_date") >= F.lit("2024-05-14").cast("date")), "left_anti")
)

deduped = (
    filtered
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.min("approved_amount").alias("min_claim_approved_amount"),
        F.count("*").alias("claim_count"),
    )
)

profiled = (
    deduped.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        "c.min_claim_approved_amount",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

claim_stats_2 = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.count(F.when(F.col("claim_type") != "medical", 1)).alias("claim_claim_type_count"),
        F.count("*").alias("claim_count_2"),
    )
)

with_stats = (
    profiled.alias("s")
    .join(claim_stats_2.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_claim_type_count"), F.lit(0)).alias("claim_claim_type_count"),
        F.coalesce(F.col("c.claim_count_2"), F.lit(0)).alias("claim_count_2"),
    )
)

latest = (
    with_stats
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest
    .filter(F.col("days_since_reported_at") < 525)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "status",
        "claim_type_or_unknown",
        "days_since_reported_at",
        "status_slug",
        "min_claim_approved_amount",
        "claim_count",
        "claim_claim_type_count",
        "claim_count_2",
        F.round(F.col("claimed_amount") * 1.07, 2).alias("claimed_amount_adj"),
        F.date_trunc("quarter", F.col("claim_date")).alias("claim_date_quarter"),
    )
    .orderBy(
        F.col("claim_claim_type_count").desc_nulls_first(),
        F.col("id").asc_nulls_last(),
    )
    .limit(5)
)
