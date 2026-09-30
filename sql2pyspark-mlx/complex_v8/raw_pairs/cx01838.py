from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_by_claim_date = Window.partitionBy("adjuster_id").orderBy(F.col("claim_date").desc(), "id")
w_all_by_claim_date = Window.orderBy(F.col("claim_date").desc_nulls_first(), "id")
w_adjuster_id = Window.partitionBy("adjuster_id")
w_claim_type_group_adjuster_id = Window.partitionBy("claim_type_group", "adjuster_id").orderBy(F.col("approved_amount_bucket_change").asc_nulls_last())
w_policy_id_adjuster_id_by_cla = Window.partitionBy("policy_id", "adjuster_id").orderBy("claimed_amount", "id")
w_policy_id_by_claim_date_rank = Window.partitionBy("policy_id").orderBy(F.col("claim_date_rank_filled_2").desc(), "id")
w_policy_id_adjuster_id_by_cla_2 = Window.partitionBy("policy_id", "adjuster_id").orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared = (
    df_claims
    .filter(F.to_date(F.col("reported_at")) < F.lit("2025-02-12").cast("date"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "status",
        F.when(F.col("status").isin("APPROVED", "PAID"), "group_a").when(F.col("status") == "REJECTED", "group_b").otherwise("other").alias("status_group"),
        (F.floor(F.col("approved_amount") / 10000) * 10000).alias("approved_amount_bucket"),
        F.when(F.col("claim_type").isin("medical", "delay"), "group_a").when(F.col("claim_type") == "theft", "group_b").otherwise("other").alias("claim_type_group"),
    )
)

metrics = (
    prepared
    .select(
        "*",
        (F.col("approved_amount_bucket") - F.lag("approved_amount_bucket").over(w_adjuster_id_by_claim_date)).alias("approved_amount_bucket_change"),
        F.rank().over(w_all_by_claim_date).alias("claim_date_rank"),
        F.min("approved_amount_bucket").over(w_adjuster_id).alias("adjuster_id_min_approved_amount_bucket"),
    )
)

derived = (
    metrics
    .filter(F.col("adjuster_id").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "status",
        "claim_type_group",
        "approved_amount_bucket_change",
        "adjuster_id_min_approved_amount_bucket",
        F.coalesce(F.col("claim_date_rank"), F.lit(0)).alias("claim_date_rank_filled"),
        F.coalesce(F.col("claim_date_rank"), F.lit(0)).alias("claim_date_rank_filled_2"),
        F.round(F.col("approved_amount_bucket") / F.nullif(F.col("claimed_amount"), F.lit(0)), 3).alias("approved_amount_bucket_per_claimed_amount"),
    )
)

sequenced = (
    derived
    .select(
        "*",
        F.dense_rank().over(w_claim_type_group_adjuster_id).alias("approved_amount_bucket_change_dense_rank"),
        F.lag("claim_date_rank_filled_2").over(w_policy_id_adjuster_id_by_cla).alias("prev_claim_date_rank_filled_2"),
        F.size(F.collect_set("claim_type_group").over(w_adjuster_id)).alias("distinct_claim_type_group_in_adjuster_id"),
        F.lag("claim_date_rank_filled_2").over(w_policy_id_by_claim_date_rank).alias("prev_claim_date_rank_filled_2_2"),
        F.count(F.lit(1)).over(w_adjuster_id).alias("rows_in_adjuster_id"),
        F.first("claim_date_rank_filled_2").over(w_policy_id_adjuster_id_by_cla_2).alias("first_claim_date_rank_filled_2"),
    )
)

summary = (
    sequenced
    .groupBy("policy_id", "first_claim_date_rank_filled_2", "adjuster_id")
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.stddev("distinct_claim_type_group_in_adjuster_id"), 2).alias("stddev_distinct_claim_type_group_in_adjuster_id"),
        F.countDistinct("id").alias("distinct_ids"),
        F.array_sort(F.collect_set("status")).alias("status_list"),
        F.min("claim_date_rank_filled_2").alias("min_claim_date_rank_filled_2"),
        F.sum("claimed_amount").alias("total_claimed_amount"),
    )
)

result = (
    summary
    .cube("distinct_ids", "min_claim_date_rank_filled_2")
    .agg(F.countDistinct("adjuster_id").alias("distinct_adjuster_ids"))
    .orderBy(
        F.col("distinct_ids").asc_nulls_last(),
        F.col("min_claim_date_rank_filled_2").asc_nulls_last(),
    )
)
