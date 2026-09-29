from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_end_date = Window.orderBy("end_date", "id")
w_status_by_premium = Window.partitionBy("status").orderBy(F.col("premium").asc_nulls_last(), "id")
w_status = Window.partitionBy("status")
w_all_by_end_date_full = Window.orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_premium_tier_by_end_date_pri = Window.partitionBy("premium_tier").orderBy("end_date", "id").rowsBetween(-3, -1)
w_premium_tier_2_status_by_pre = Window.partitionBy("premium_tier_2", "status").orderBy(F.col("premium").desc())
w_premium_tier_by_end_date = Window.partitionBy("premium_tier").orderBy("end_date", "id")
w_premium_tier_2_status_by_pre_2 = Window.partitionBy("premium_tier_2", "status").orderBy(F.col("premium").desc(), "id")
w_all_by_end_date_prior_6 = Window.orderBy("end_date", "id").rowsBetween(-6, -1)
w_status_by_premium_prior_3 = Window.partitionBy("status").orderBy(F.col("premium").asc_nulls_last(), "id").rowsBetween(-3, -1)
w_premium_rank_cnt_by_second_e = Window.partitionBy("premium_rank", "cnt").orderBy("second_end_date_span_days", "premium_rank").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_premium_rank_cnt_by_second_e_2 = Window.partitionBy("premium_rank", "cnt").orderBy("second_end_date_span_days", "premium_rank").rowsBetween(-3, -1)
w_cnt_premium_rank = Window.partitionBy("cnt", "premium_rank")

prepared = (
    df_policies
    .filter(F.col("premium") > 1042.8)
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.when(F.col("premium") >= 3544.1, "large").when(F.col("premium") >= 2096.6, "medium").otherwise("small").alias("premium_tier"),
        F.when(F.col("premium") >= 3532.7, "HIGH").when(F.col("premium") >= 2111.3, "MEDIUM").otherwise("LOW").alias("premium_tier_2"),
    )
)

windowed = (
    prepared
    .select(
        "id",
        "holder_id",
        "end_date",
        "premium_tier_2",
        F.lag("start_date").over(w_all_by_end_date).alias("prev_start_date"),
        F.datediff(F.col("start_date"), F.lag("start_date").over(w_status_by_premium)).alias("days_since_prev_start_date"),
        F.count(F.lit(1)).over(w_status).alias("rows_in_status"),
        F.nth_value("end_date", 2).over(w_all_by_end_date_full).alias("second_end_date"),
        F.sum("premium").over(w_premium_tier_by_end_date_pri).alias("premium_sum_3_prior"),
        F.rank().over(w_premium_tier_2_status_by_pre).alias("premium_rank"),
        F.ntile(3).over(w_premium_tier_by_end_date).alias("end_date_tercile"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_status), F.lit(0)), 4).alias("premium_share_of_status"),
        F.lead("premium", 2).over(w_premium_tier_2_status_by_pre_2).alias("next2_premium"),
        F.sum("premium").over(w_status).alias("status_sum_premium"),
        F.dense_rank().over(w_premium_tier_2_status_by_pre_2).alias("premium_dense_rank"),
        F.round(F.avg("premium").over(w_all_by_end_date_prior_6), 2).alias("premium_avg_6_prior"),
        F.round(F.avg("premium").over(w_status_by_premium_prior_3), 2).alias("premium_avg_3_prior"),
    )
)

stats = (
    windowed
    .filter(F.col("holder_id") <= 11)
    .groupBy("premium_rank")
    .agg(
        F.datediff(F.max("second_end_date"), F.min("second_end_date")).alias("second_end_date_span_days"),
        F.count("*").alias("cnt"),
        F.round(F.sum("next2_premium"), 2).alias("total_next2_premium"),
        F.percentile_approx("status_sum_premium", 0.5).alias("median_status_sum_premium"),
    )
)

with_trends = (
    stats
    .select(
        "*",
        F.first("second_end_date_span_days").over(w_premium_rank_cnt_by_second_e).alias("first_second_end_date_span_days"),
        F.sum("second_end_date_span_days").over(w_premium_rank_cnt_by_second_e_2).alias("second_end_date_span_days_sum_3_prior"),
        F.round(F.avg("total_next2_premium").over(w_cnt_premium_rank), 2).alias("cnt_premium_rank_avg_total_next2_premium"),
        F.round(F.col("second_end_date_span_days") / F.nullif(F.sum("second_end_date_span_days").over(w_cnt_premium_rank), F.lit(0)), 4).alias("second_end_date_span_days_share_of_cnt_premium_r"),
    )
)

result = (
    with_trends
    .select(
        "premium_rank",
        "second_end_date_span_days",
        "cnt",
        "median_status_sum_premium",
        "second_end_date_span_days_sum_3_prior",
        "second_end_date_span_days_share_of_cnt_premium_r",
        F.round(F.col("total_next2_premium") * 1.07, 2).alias("total_next2_premium_adj"),
        F.coalesce(F.col("total_next2_premium"), F.lit(0.0)).alias("total_next2_premium_filled"),
    )
)
