from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_start_date_year_by = Window.partitionBy("holder_id", F.year("start_date")).orderBy(F.col("start_date").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id_start_date_year_by_2 = Window.partitionBy("holder_id", F.year("start_date")).orderBy(F.col("start_date").desc_nulls_first(), "id")
w_holder_id_start_date_year_by_3 = Window.partitionBy("holder_id", F.year("start_date")).orderBy(F.col("start_date").desc_nulls_first())
w_holder_id_start_date_year_by_4 = Window.partitionBy("holder_id", F.year("start_date")).orderBy(F.col("start_date").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_product = Window.partitionBy("status", "product")
w_status_product_by_end_date_m = Window.partitionBy("status", "product").orderBy("end_date", "id").rowsBetween(-1, Window.currentRow)
w_status_product_by_end_date_f = Window.partitionBy("status", "product").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_product_by_premium_distance = Window.partitionBy("product").orderBy(F.col("premium_distance").desc(), "id").rowsBetween(-2, -1)
w_status_product_by_end_date_t = Window.partitionBy("status", "product").orderBy("end_date")
w_end_date_year = Window.partitionBy(F.year("end_date"))
w_product_by_start_date_prior = Window.partitionBy("product").orderBy(F.col("start_date").asc_nulls_last(), "id").rowsBetween(-1, -1)
w_all_by_running_premium = Window.orderBy(F.col("running_premium").desc(), "id")

prepared = (
    df_policies
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.abs(F.col("premium") - 3521.8).alias("premium_distance"),
    )
)

windowed = (
    prepared
    .select(
        "*",
        F.sum("premium").over(w_holder_id_start_date_year_by).alias("running_premium"),
        F.datediff(F.col("end_date"), F.lag("end_date").over(w_holder_id_start_date_year_by_2)).alias("days_since_prev_end_date"),
        F.round(F.percent_rank().over(w_holder_id_start_date_year_by_3), 3).alias("start_date_percent_rank"),
        F.nth_value("premium_distance", 2).over(w_holder_id_start_date_year_by_4).alias("second_premium_distance"),
        F.dense_rank().over(w_holder_id_start_date_year_by_3).alias("start_date_dense_rank"),
        F.round(F.col("premium_distance") / F.nullif(F.sum("premium_distance").over(w_status_product), F.lit(0)), 4).alias("premium_distance_share_of_status_product"),
        F.round(F.avg("premium_distance").over(w_status_product_by_end_date_m), 2).alias("premium_distance_avg_2"),
        F.sum("premium_distance").over(w_status_product).alias("status_product_sum_premium_distance"),
        F.nth_value("premium", 2).over(w_status_product_by_end_date_f).alias("second_premium"),
        F.round(F.avg("premium_distance").over(w_product_by_premium_distance), 2).alias("premium_distance_avg_2_prior"),
        F.round(F.col("premium") - F.avg("premium").over(w_status_product), 2).alias("premium_vs_status_product_avg"),
        F.size(F.collect_set("holder_id").over(w_status_product)).alias("distinct_holder_id_in_status_product"),
        F.rank().over(w_status_product_by_end_date_t).alias("end_date_rank"),
    )
)

ranked = (
    windowed
    .select(
        "*",
        F.sum("premium").over(w_end_date_year).alias("end_date_year_sum_premium"),
        F.sum("start_date_percent_rank").over(w_product_by_start_date_prior).alias("start_date_percent_rank_sum_1_prior"),
        F.lag("end_date_rank", 3, 0).over(w_all_by_running_premium).alias("prev3_end_date_rank"),
        F.sum("end_date_rank").over(w_end_date_year).alias("end_date_year_sum_end_date_rank"),
        F.size(F.collect_set("holder_id").over(w_end_date_year)).alias("distinct_holder_id_in_end_date_year"),
    )
)

result = (
    ranked
    .select(
        "id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        "premium_distance",
        "running_premium",
        "days_since_prev_end_date",
        "start_date_percent_rank",
        "start_date_dense_rank",
        "premium_distance_avg_2",
        "premium_distance_avg_2_prior",
        "premium_vs_status_product_avg",
        "distinct_holder_id_in_status_product",
        "end_date_rank",
        "end_date_year_sum_premium",
        "start_date_percent_rank_sum_1_prior",
        "end_date_year_sum_end_date_rank",
        "distinct_holder_id_in_end_date_year",
        (F.floor(F.col("second_premium_distance") / 1000) * 1000).alias("second_premium_distance_bucket"),
        (F.floor(F.col("end_date_rank") / 2) * 2).alias("end_date_rank_bucket"),
    )
    .orderBy(
        F.col("distinct_holder_id_in_end_date_year").desc(),
        F.col("premium_distance_avg_2_prior").asc_nulls_last(),
    )
)
