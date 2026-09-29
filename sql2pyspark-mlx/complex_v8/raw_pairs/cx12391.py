from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_start_date = Window.orderBy("start_date", "id")
w_all_by_start_date_moving_1 = Window.orderBy("start_date", "id").rowsBetween(-1, Window.currentRow)
w_all_by_start_date_prior_2 = Window.orderBy("start_date", "id").rowsBetween(-2, -1)
w_all_by_start_date_ties = Window.orderBy("start_date")
w_holder_id_product_by_next_st = Window.partitionBy("holder_id", "product").orderBy(F.col("next_start_date").desc_nulls_first(), "id").rowsBetween(-3, -1)
w_premium_tier_status = Window.partitionBy("premium_tier", "status")
w_holder_id_product_by_next_st_2 = Window.partitionBy("holder_id", "product").orderBy(F.col("next_start_date").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_premium_sum_2 = Window.orderBy(F.col("premium_sum_2").desc(), "id")
w_dedup = Window.partitionBy("premium_tier", "holder_segment").orderBy("avg_policy_premium", "id")
w_premium_tier_by_premium_avg = Window.partitionBy("premium_tier").orderBy("premium_avg_2_prior_share_of_premium_tier_status")
w_holder_segment_product = Window.partitionBy("holder_segment", "product")
w_all_by_avg_policy_premium = Window.orderBy(F.col("avg_policy_premium").desc(), "id")
w_premium_tier_by_premium_avg_2 = Window.partitionBy("premium_tier").orderBy("premium_avg_2_prior_share_of_premium_tier_status", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_policys = (
    df_policies
    .filter(F.col("status") == "ACTIVE")
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
        F.when(F.col("premium") >= 3465.4, "large").when(F.col("premium") >= 1541.8, "medium").otherwise("small").alias("premium_tier"),
    )
)

windowed = (
    prepared_policys
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "status",
        "premium_tier",
        F.lead("start_date").over(w_all_by_start_date).alias("next_start_date"),
        F.sum("premium").over(w_all_by_start_date_moving_1).alias("premium_sum_2"),
        F.round(F.avg("premium").over(w_all_by_start_date_prior_2), 2).alias("premium_avg_2_prior"),
        F.round(F.cume_dist().over(w_all_by_start_date_ties), 3).alias("start_date_cume_dist"),
    )
)

with_trends = (
    windowed
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "premium_tier",
        "next_start_date",
        "premium_sum_2",
        "premium_avg_2_prior",
        "start_date_cume_dist",
        F.max("start_date_cume_dist").over(w_holder_id_product_by_next_st).alias("start_date_cume_dist_max_3_prior"),
        F.round(F.col("premium_avg_2_prior") / F.nullif(F.sum("premium_avg_2_prior").over(w_premium_tier_status), F.lit(0)), 4).alias("premium_avg_2_prior_share_of_premium_tier_status"),
        F.min("premium").over(w_premium_tier_status).alias("premium_tier_status_min_premium"),
        F.nth_value("next_start_date", 2).over(w_holder_id_product_by_next_st_2).alias("second_next_start_date"),
        F.rank().over(w_all_by_premium_sum_2).alias("premium_sum_2_rank"),
    )
)

with_parent_holder = (
    with_trends.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.joined_on") < F.lit("2024-12-21").cast("date")), "inner")
    .filter(F.col("p.segment") == "VIP")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium_tier",
        "s.premium_sum_2",
        "s.premium_avg_2_prior",
        "s.start_date_cume_dist",
        "s.start_date_cume_dist_max_3_prior",
        "s.premium_avg_2_prior_share_of_premium_tier_status",
        "s.premium_sum_2_rank",
        F.col("p.segment").alias("holder_segment"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.count(F.when(F.col("status") != "CANCELLED", 1)).alias("policy_status_count"),
        F.round(F.avg("premium"), 2).alias("avg_policy_premium"),
    )
)

with_stats = (
    with_parent_holder.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.policy_status_count"), F.lit(0)).alias("policy_status_count"),
        "p.avg_policy_premium",
    )
)

deduped = (
    with_stats
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

sequenced = (
    deduped
    .select(
        "*",
        F.round(F.cume_dist().over(w_premium_tier_by_premium_avg), 3).alias("premium_avg_2_prior_share_of_premium_tier_status_2"),
        F.round(F.col("premium_sum_2_rank") - F.avg("premium_sum_2_rank").over(w_holder_segment_product), 2).alias("premium_sum_2_rank_vs_holder_segment_product_avg"),
        F.lag("avg_policy_premium", 1, 0).over(w_all_by_avg_policy_premium).alias("prev_avg_policy_premium"),
        F.first("premium_avg_2_prior", ignorenulls=True).over(w_premium_tier_by_premium_avg_2).alias("first_known_premium_avg_2_prior"),
        F.max("premium_sum_2").over(w_holder_segment_product).alias("holder_segment_product_max_premium_sum_2"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "holder_id",
        "premium_tier",
        "premium_sum_2",
        "premium_avg_2_prior",
        "start_date_cume_dist_max_3_prior",
        "premium_avg_2_prior_share_of_premium_tier_status",
        "premium_sum_2_rank",
        "holder_segment",
        "policy_status_count",
        "avg_policy_premium",
        "premium_avg_2_prior_share_of_premium_tier_status_2",
        "premium_sum_2_rank_vs_holder_segment_product_avg",
        "prev_avg_policy_premium",
        "first_known_premium_avg_2_prior",
        "holder_segment_product_max_premium_sum_2",
        F.coalesce(F.col("avg_policy_premium"), F.lit(0.0)).alias("avg_policy_premium_filled"),
        F.lower(F.trim(F.col("holder_segment"))).alias("holder_segment_clean"),
    )
    .orderBy(F.col("premium_tier").desc(), "prev_avg_policy_premium", "id")
    .limit(5)
)
