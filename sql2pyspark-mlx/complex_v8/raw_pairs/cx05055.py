from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("status", "policy_id").orderBy(F.col("claimed_amount").desc(), F.col("id").desc())

prepared = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "status",
        F.length(F.trim(F.col("status"))).alias("status_len"),
        F.substring(F.trim(F.col("status")), 1, 3).alias("status_prefix"),
        F.concat(F.col("claim_type"), F.lit("#"), F.col("adjuster_id").cast("string")).alias("claim_type_ref"),
    )
)

filtered = (
    prepared.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.reported_at") >= F.lit("2025-02-06 15:00:00").cast("timestamp")), "left_anti")
    .filter(F.col("policy_id").isNotNull())
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.count("*").alias("claim_count"),
        F.sum("claimed_amount").alias("total_claim_claimed_amount"),
        F.min("claimed_amount").alias("min_claim_claimed_amount"),
    )
)

with_counts = (
    filtered.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        "c.total_claim_claimed_amount",
        "c.min_claim_claimed_amount",
    )
)

latest_per_key = (
    with_counts
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest_per_key
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "status",
        "status_len",
        "status_prefix",
        "claim_type_ref",
        "min_claim_claimed_amount",
        F.date_format(F.col("claim_date"), "yyyy-MM-dd").alias("claim_date_label"),
    )
    .orderBy("claimed_amount")
)
