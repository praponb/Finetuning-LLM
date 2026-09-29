from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("claim_type", "status").orderBy(F.col("claimed_amount_distance").desc(), F.col("id").desc())

scoped = (
    df_claims
    .filter((F.col("claim_type").isin("flood", "theft")) & (F.col("status") == "REJECTED"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
        F.trunc(F.col("claim_date"), "MM").alias("claim_date_month_start"),
        F.abs(F.col("claimed_amount") - 20461.6).alias("claimed_amount_distance"),
    )
)

deduped = (
    scoped
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count("*").alias("claim_count"),
        F.max("claim_date").alias("last_claim_date"),
        F.min("approved_amount").alias("min_claim_approved_amount"),
    )
)

with_counts = (
    deduped.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        "c.last_claim_date",
        "c.min_claim_approved_amount",
    )
)

without_matches = (
    with_counts
    .crossJoin(with_counts.agg(F.min("min_claim_approved_amount").alias("overall_min_min_claim_approved_amount")))
    .filter(F.col("min_claim_approved_amount") > F.col("overall_min_min_claim_approved_amount"))
    .drop("overall_min_min_claim_approved_amount")
)

labeled = (
    without_matches
    .select(
        "*",
        F.concat(F.col("claim_type"), F.lit("#"), F.col("policy_id").cast("string")).alias("claim_type_ref"),
        F.datediff(F.col("last_claim_date"), F.col("claim_date_month_start")).alias("days_claim_date_month_start_to_last_claim_date"),
        F.abs(F.col("claim_count") - 3).alias("claim_count_distance"),
    )
)

claim_stats_2 = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.max("claim_date").alias("last_claim_date_2"),
        F.round(F.avg("claimed_amount"), 2).alias("avg_claim_claimed_amount"),
        F.count("*").alias("claim_count_2"),
    )
)

summarized_rows = (
    labeled.alias("s")
    .join(claim_stats_2.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        "c.last_claim_date_2",
        "c.avg_claim_claimed_amount",
        F.coalesce(F.col("c.claim_count_2"), F.lit(0)).alias("claim_count_2"),
    )
)

result = (
    summarized_rows
    .select("adjuster_id")
    .distinct()
    .orderBy(F.col("adjuster_id").desc())
)
