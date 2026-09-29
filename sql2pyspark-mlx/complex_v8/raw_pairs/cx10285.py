from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_or_unknown_holder_id = Window.partitionBy("status_or_unknown", "holder_id")
w_holder_id_product_by_premium = Window.partitionBy("holder_id", "product").orderBy(F.col("premium").desc(), "id")
w_all_by_start_date_running = Window.orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_product_by_premium = Window.partitionBy("product").orderBy("premium", "id")
w_holder_id_product_by_premium_2 = Window.partitionBy("holder_id", "product").orderBy(F.col("premium").desc(), "id").rowsBetween(-6, -1)
w_product_by_premium_moving_2 = Window.partitionBy("product").orderBy("premium", "id").rowsBetween(-2, Window.currentRow)
w_all_by_start_date_full = Window.orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_start_date_moving_2 = Window.orderBy("start_date", "id").rowsBetween(-2, Window.currentRow)
w_product_by_premium_running = Window.partitionBy("product").orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_product_by_premium_ties = Window.partitionBy("product").orderBy("premium")
w_status_or_unknown_premium_se = Window.partitionBy("status_or_unknown", "premium_seq").orderBy("premium_sum_3", "id")
w_status_or_unknown_premium_ra = Window.partitionBy("status_or_unknown", "premium_rank")
w_premium_rank_2_by_premium_av = Window.partitionBy("premium_rank_2").orderBy(F.col("premium_avg_3").desc(), "id")
w_all_by_premium_avg_3_moving = Window.orderBy("premium_avg_3", "id").rowsBetween(-1, Window.currentRow)
w_holder_id_status_or_unknown = Window.partitionBy("holder_id", "status_or_unknown")
w_premium_seq_status_or_unknow = Window.partitionBy("premium_seq", "status_or_unknown").orderBy("premium_sum_3", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_policys = (
    df_policies
    .filter(F.col("product").isin("LIFE", "AUTO", "HEALTH"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
        F.substring(F.trim(F.col("status")), 1, 2).alias("status_prefix"),
    )
)

windowed = (
    base_policys
    .select(
        "id",
        "holder_id",
        "premium",
        "status_or_unknown",
        F.count(F.lit(1)).over(w_status_or_unknown_holder_id).alias("rows_in_status_or_unknown_holder_id"),
        F.row_number().over(w_holder_id_product_by_premium).alias("premium_seq"),
        F.first("premium", ignorenulls=True).over(w_all_by_start_date_running).alias("first_known_premium"),
        F.rank().over(w_product_by_premium).alias("premium_rank"),
        F.min("premium").over(w_status_or_unknown_holder_id).alias("status_or_unknown_holder_id_min_premium"),
        F.max("premium").over(w_status_or_unknown_holder_id).alias("status_or_unknown_holder_id_max_premium"),
        F.round(F.avg("premium").over(w_holder_id_product_by_premium_2), 2).alias("premium_avg_6_prior"),
        F.sum("premium").over(w_product_by_premium_moving_2).alias("premium_sum_3"),
        F.nth_value("start_date", 2).over(w_all_by_start_date_full).alias("second_start_date"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_status_or_unknown_holder_id), F.lit(0)), 4).alias("premium_share_of_status_or_unknown_holder_id"),
        F.round(F.avg("premium").over(w_all_by_start_date_moving_2), 2).alias("premium_avg_3"),
        F.last("premium", ignorenulls=True).over(w_product_by_premium_running).alias("last_known_premium"),
        F.rank().over(w_product_by_premium_ties).alias("premium_rank_2"),
        F.round(F.percent_rank().over(w_holder_id_product_by_premium), 3).alias("premium_percent_rank"),
    )
)

with_trends = (
    windowed
    .select(
        "id",
        "holder_id",
        "premium",
        "rows_in_status_or_unknown_holder_id",
        "premium_seq",
        "first_known_premium",
        "premium_rank",
        "premium_avg_6_prior",
        "premium_sum_3",
        "second_start_date",
        "last_known_premium",
        "premium_rank_2",
        "premium_percent_rank",
        F.ntile(10).over(w_status_or_unknown_premium_se).alias("premium_sum_3_decile"),
        F.size(F.collect_set("holder_id").over(w_status_or_unknown_premium_ra)).alias("distinct_holder_id_in_status_or_unknown_premium_"),
        F.ntile(3).over(w_premium_rank_2_by_premium_av).alias("premium_avg_3_tercile"),
        F.round(F.avg("premium_avg_3").over(w_all_by_premium_avg_3_moving), 2).alias("premium_avg_3_avg_2"),
        F.last("premium", ignorenulls=True).over(w_premium_seq_status_or_unknow).alias("last_known_premium_2"),
        F.max("premium_avg_6_prior").over(w_status_or_unknown_premium_ra).alias("status_or_unknown_premium_rank_max_premium_avg_6"),
    )
)

summary = (
    with_trends
    .groupBy("premium_seq", "premium_avg_3_tercile", "holder_id")
    .agg(
        F.percentile_approx("distinct_holder_id_in_status_or_unknown_premium_", 0.5).alias("median_distinct_holder_id_in_status_or_unknown_p"),
        F.min("second_start_date").alias("first_second_start_date"),
    )
)

result = (
    summary
    .select(
        "premium_seq",
        "premium_avg_3_tercile",
        "holder_id",
        "first_second_start_date",
        F.round(F.col("premium_avg_3_tercile") / F.nullif(F.col("premium_seq"), F.lit(0)), 3).alias("premium_avg_3_tercile_per_premium_seq"),
    )
    .orderBy(F.col("premium_avg_3_tercile_per_premium_seq").asc(), "holder_id")
)
