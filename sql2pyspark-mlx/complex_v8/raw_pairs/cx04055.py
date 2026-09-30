from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_row_count = Window.partitionBy("row_count")
w_all_by_row_count = Window.orderBy(F.col("row_count").asc_nulls_last(), "policy_id", "claim_type_group")

cleaned = (
    df_claims
    .filter(F.col("adjuster_id") >= 7)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.when(F.col("claim_type").isin("flood", "theft"), "group_a").when(F.col("claim_type") == "fire", "group_b").otherwise("other").alias("claim_type_group"),
        F.round(F.col("approved_amount") * 1.5, 2).alias("approved_amount_adj"),
        F.abs(F.col("approved_amount") - 9389.3).alias("approved_amount_distance"),
    )
)

stats = (
    cleaned
    .groupBy("policy_id", "claim_type_group")
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("adjuster_id").alias("distinct_adjuster_ids"),
        F.countDistinct("claim_type_group").alias("distinct_claim_type_groups"),
        F.sum(F.when(F.col("status").isin("IN_REVIEW", "REJECTED"), 1).otherwise(0)).alias("n_status_in_in_review_rejected"),
    )
)

with_activity = (
    stats.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.product") == "HEALTH"), "left_anti")
)

with_trends = (
    with_activity
    .withColumn("distinct_policy_id_in_row_count", F.size(F.collect_set("policy_id").over(w_row_count)))
    .withColumn("row_count_dense_rank", F.dense_rank().over(w_all_by_row_count))
)

result = (
    with_trends
    .groupingSets(
        [["row_count_dense_rank", "policy_id", "n_status_in_in_review_rejected"], ["row_count_dense_rank"]],
        "row_count_dense_rank", "policy_id", "n_status_in_in_review_rejected",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.sum("row_count_dense_rank").alias("total_row_count_dense_rank"),
        F.percentile_approx("distinct_adjuster_ids", 0.5).alias("median_distinct_adjuster_ids"),
        F.max("distinct_claim_type_groups").alias("max_distinct_claim_type_groups"),
    )
    .orderBy(
        F.col("row_count_dense_rank").asc_nulls_last(),
        F.col("policy_id").asc_nulls_last(),
        F.col("n_status_in_in_review_rejected").asc_nulls_last(),
    )
)
