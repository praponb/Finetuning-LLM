from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_count_claimed_amount_t = Window.partitionBy("claim_count", "claimed_amount_tier")
w_all_by_claim_count_2_moving = Window.orderBy("claim_count_2", "id").rowsBetween(-2, Window.currentRow)

valid_claims = (
    df_claims
    .filter(F.col("approved_amount") <= 17759.3)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.round(F.col("claimed_amount") * 1.07, 2).alias("claimed_amount_adj"),
        F.date_format(F.col("claim_date"), "MMM yyyy").alias("claim_date_label"),
        F.when(F.col("claimed_amount") >= 42447.2, "large").when(F.col("claimed_amount") >= 20436.5, "medium").otherwise("small").alias("claimed_amount_tier"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(F.count("*").alias("claim_count"), F.count("*").alias("claim_count_2"))
)

summarized_rows = (
    valid_claims.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        F.coalesce(F.col("c.claim_count_2"), F.lit(0)).alias("claim_count_2"),
    )
)

claim_stats_2 = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.count("*").alias("claim_count_3"),
        F.count(F.when(F.col("status").isin("OPEN", "PAID"), 1)).alias("claim_status_count"),
        F.count("*").alias("claim_count_4"),
    )
)

with_counts = (
    summarized_rows.alias("s")
    .join(claim_stats_2.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count_3"), F.lit(0)).alias("claim_count_3"),
        F.coalesce(F.col("c.claim_status_count"), F.lit(0)).alias("claim_status_count"),
        F.coalesce(F.col("c.claim_count_4"), F.lit(0)).alias("claim_count_4"),
    )
)

metrics = (
    with_counts
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "status",
        "reported_at",
        "claimed_amount_adj",
        "claim_count",
        "claim_count_2",
        "claim_status_count",
        "claim_count_4",
        F.size(F.collect_set("claim_type").over(w_claim_count_claimed_amount_t)).alias("distinct_claim_type_in_claim_count_claimed_amoun"),
        F.round(F.avg("claim_count_2").over(w_all_by_claim_count_2_moving), 2).alias("claim_count_2_avg_3"),
    )
)

result = (
    metrics
    .groupBy("policy_id", "reported_at")
    .pivot("status", ["REJECTED", "OPEN", "PAID", "IN_REVIEW"])
    .agg(F.sum("claim_count_2_avg_3"))
)
