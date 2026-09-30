from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_claim_count_by_claim = Window.partitionBy("status", "claim_count").orderBy(F.col("claim_date").desc_nulls_first(), "id").rowsBetween(-3, Window.currentRow)
w_claim_type_by_claim_count_pr = Window.partitionBy("claim_type").orderBy("claim_count", "id").rowsBetween(-6, -1)
w_policy_id = Window.partitionBy("policy_id")
w_latest = Window.partitionBy("policy_id").orderBy("claim_count_3", "id", "claim_id")

scoped_claims = (
    df_claims
    .filter((F.col("claimed_amount") > 3703.0) & (F.col("reported_at") >= F.lit("2025-01-25 11:00:00").cast("timestamp")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.min("claimed_amount").alias("min_claim_claimed_amount"),
        F.count("*").alias("claim_count"),
        F.max("reported_at").alias("last_claim_reported_at"),
    )
)

summarized_rows = (
    scoped_claims.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        "c.min_claim_claimed_amount",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        "c.last_claim_reported_at",
    )
)

windowed = (
    summarized_rows
    .select(
        "*",
        F.round(F.avg("claimed_amount").over(w_status_claim_count_by_claim), 2).alias("claimed_amount_avg_4"),
        F.round(F.avg("claim_count").over(w_claim_type_by_claim_count_pr), 2).alias("claim_count_avg_6_prior"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_policy_id), 2).alias("claimed_amount_vs_policy_id_avg"),
        F.size(F.collect_set("status").over(w_policy_id)).alias("distinct_status_in_policy_id"),
    )
)

claim_stats_2 = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.count("*").alias("claim_count_2"),
        F.count("*").alias("claim_count_3"),
        F.count("*").alias("claim_count_4"),
    )
)

with_stats = (
    windowed.alias("s")
    .join(claim_stats_2.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count_2"), F.lit(0)).alias("claim_count_2"),
        F.coalesce(F.col("c.claim_count_3"), F.lit(0)).alias("claim_count_3"),
        F.coalesce(F.col("c.claim_count_4"), F.lit(0)).alias("claim_count_4"),
    )
)

exploded_detail = (
    with_stats.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (~F.col("c.claim_type").isin("fire", "delay")), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.claimed_amount",
        "s.status",
        "s.claim_count",
        "s.last_claim_reported_at",
        "s.claimed_amount_avg_4",
        "s.claim_count_avg_6_prior",
        "s.claim_count_2",
        "s.claim_count_3",
        F.col("c.id").alias("claim_id"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.claim_type").alias("claim_claim_type"),
    )
)

latest = (
    exploded_detail
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest
    .groupBy("adjuster_id", "claim_count")
    .pivot("claim_type", ["collision", "theft", "fire"])
    .agg(F.avg("claim_count_3"))
)
