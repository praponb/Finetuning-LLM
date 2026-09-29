from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("adjuster_id", "policy_id").orderBy(F.col("policy_start_date").desc(), F.col("id").desc(), F.col("claim_id").desc())

filtered = (
    df_claims
    .filter((F.col("claimed_amount").between(5313.8, 43724.9)) & (F.col("reported_at") < F.lit("2025-02-18 13:00:00").cast("timestamp")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("claim_date")).alias("days_since_claim_date"),
    )
)

detail_rows = (
    filtered.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.days_since_claim_date",
        F.col("c.id").alias("claim_id"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.claim_type").alias("claim_claim_type"),
    )
)

joined_policy = (
    detail_rows.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.days_since_claim_date",
        "s.claim_id",
        "s.claim_reported_at",
        F.col("p.premium").alias("policy_premium"),
        F.col("p.start_date").alias("policy_start_date"),
    )
)

latest_per_key = (
    joined_policy
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

tagged = (
    latest_per_key
    .select(
        "id",
        "claimed_amount",
        "days_since_claim_date",
        "claim_id",
        "claim_reported_at",
        "policy_premium",
        "policy_start_date",
        F.date_add(F.col("claim_date"), 30).alias("claim_date_plus_30d"),
        (F.floor(F.col("claimed_amount") / 10000) * 10000).alias("claimed_amount_bucket"),
    )
)

derived = (
    tagged
    .select(
        "*",
        F.when(F.col("days_since_claim_date") >= 490, "A").when(F.col("days_since_claim_date") >= 245, "B").otherwise("C").alias("days_since_claim_date_tier"),
        F.abs(F.col("policy_premium") - 2289.0).alias("policy_premium_distance"),
    )
)

grouped = (
    derived
    .filter(F.col("claim_date_plus_30d").isNotNull())
    .groupBy("days_since_claim_date_tier")
    .agg(
        F.round(F.stddev("claimed_amount_bucket"), 2).alias("stddev_claimed_amount_bucket"),
        F.count("*").alias("row_count"),
        F.max("claim_date_plus_30d").alias("last_claim_date_plus_30d"),
        F.countDistinct("id").alias("distinct_ids"),
        F.min("days_since_claim_date").alias("min_days_since_claim_date"),
        F.sum("days_since_claim_date").alias("sum_days_since_claim_date_check"),
    )
    .filter(F.col("sum_days_since_claim_date_check") > 105)
    .drop("sum_days_since_claim_date_check")
)

result = (
    grouped
    .filter(F.col("stddev_claimed_amount_bucket") > 17500.0)
    .select(
        "days_since_claim_date_tier",
        "stddev_claimed_amount_bucket",
        "row_count",
        "min_days_since_claim_date",
        F.quarter(F.col("last_claim_date_plus_30d")).alias("last_claim_date_plus_30d_quarter"),
    )
    .orderBy(F.col("row_count").desc())
)
