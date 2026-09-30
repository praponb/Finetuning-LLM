from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_holder_id_by_premium = Window.partitionBy("product", "holder_id").orderBy("premium", "id")
w_holder_id_by_premium_moving = Window.partitionBy("holder_id").orderBy("premium", "id").rowsBetween(-6, Window.currentRow)
w_start_date_month_product_by = Window.partitionBy(F.month("start_date"), "product").orderBy(F.col("premium_distance").desc(), "id").rowsBetween(-2, -1)
w_product_end_date_month = Window.partitionBy("product", F.month("end_date"))
w_holder_id_product_by_premium = Window.partitionBy("holder_id", "product").orderBy(F.col("premium_distance").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_end_date_prior_3 = Window.orderBy(F.col("end_date").desc(), "id").rowsBetween(-3, -1)
w_product_start_date_month = Window.partitionBy("product", F.month("start_date"))
w_holder_id_product_by_premium_2 = Window.partitionBy("holder_id", "product").orderBy(F.col("premium_distance").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_start_date_month_product_by_2 = Window.partitionBy(F.month("start_date"), "product").orderBy(F.col("premium_distance").desc(), "id")
w_holder_id_by_premium_prior_3 = Window.partitionBy("holder_id").orderBy("premium", "id").rowsBetween(-3, -1)
w_holder_id = Window.partitionBy("holder_id")
w_second_start_date_month = Window.partitionBy(F.month("second_start_date"))
w_all_by_premium_distance_avg = Window.orderBy("premium_distance_avg_3_prior")
w_all_by_premium_distance_shar = Window.orderBy("premium_distance_share_of_product_start_date_mon", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id_start_date_month_b = Window.partitionBy("holder_id", F.date_trunc("month", "start_date")).orderBy(F.col("premium_distance_avg_3_prior").desc_nulls_first())

scoped = (
    df_policies
    .filter(F.col("status") == "ACTIVE")
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        F.abs(F.col("premium") - 2289.0).alias("premium_distance"),
    )
)

sequenced = (
    scoped
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        F.row_number().over(w_product_holder_id_by_premium).alias("rn"),
        F.round(F.avg("premium").over(w_holder_id_by_premium_moving), 2).alias("premium_avg_7"),
        F.sum("premium_distance").over(w_start_date_month_product_by).alias("premium_distance_sum_2_prior"),
        F.max("premium").over(w_product_end_date_month).alias("product_end_date_month_max_premium"),
        F.nth_value("start_date", 2).over(w_holder_id_product_by_premium).alias("second_start_date"),
        F.round(F.avg("premium_distance").over(w_all_by_end_date_prior_3), 2).alias("premium_distance_avg_3_prior"),
        F.round(F.col("premium_distance") / F.nullif(F.sum("premium_distance").over(w_product_start_date_month), F.lit(0)), 4).alias("premium_distance_share_of_product_start_date_mon"),
        F.round(F.col("premium_distance") - F.avg("premium_distance").over(w_product_end_date_month), 2).alias("premium_distance_vs_product_end_date_month_avg"),
        F.first("premium_distance", ignorenulls=True).over(w_holder_id_product_by_premium_2).alias("first_known_premium_distance"),
        F.lead("end_date").over(w_start_date_month_product_by_2).alias("next_end_date"),
        F.sum("premium").over(w_product_start_date_month).alias("product_start_date_month_sum_premium"),
        F.round(F.avg("premium_distance").over(w_holder_id_by_premium_prior_3), 2).alias("premium_distance_avg_3_prior_2"),
    )
)

windowed = (
    sequenced
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "rn",
        "premium_avg_7",
        "product_end_date_month_max_premium",
        "second_start_date",
        "premium_distance_avg_3_prior",
        "premium_distance_vs_product_end_date_month_avg",
        "next_end_date",
        "product_start_date_month_sum_premium",
        "premium_distance_avg_3_prior_2",
        F.round(F.avg("premium_avg_7").over(w_holder_id), 2).alias("holder_id_avg_premium_avg_7"),
        F.min("premium_distance_sum_2_prior").over(w_second_start_date_month).alias("second_start_date_month_min_premium_distance_sum"),
        F.sum("rn").over(w_second_start_date_month).alias("second_start_date_month_sum_rn"),
        F.dense_rank().over(w_all_by_premium_distance_avg).alias("premium_distance_avg_3_prior_dense_rank"),
        F.last("premium_distance_avg_3_prior", ignorenulls=True).over(w_all_by_premium_distance_shar).alias("last_known_premium_distance_avg_3_prior"),
        F.round(F.percent_rank().over(w_holder_id_start_date_month_b), 3).alias("premium_distance_avg_3_prior_percent_rank"),
        F.round(F.col("premium_distance_avg_3_prior") - F.avg("premium_distance_avg_3_prior").over(w_second_start_date_month), 2).alias("premium_distance_avg_3_prior_vs_second_start_dat"),
        F.round(F.avg("premium_distance_sum_2_prior").over(w_second_start_date_month), 2).alias("second_start_date_month_avg_premium_distance_sum"),
    )
)

result = (
    windowed
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "premium_avg_7",
        "product_end_date_month_max_premium",
        "premium_distance_avg_3_prior",
        "next_end_date",
        "product_start_date_month_sum_premium",
        "premium_distance_avg_3_prior_2",
        "holder_id_avg_premium_avg_7",
        "second_start_date_month_min_premium_distance_sum",
        "second_start_date_month_sum_rn",
        "premium_distance_avg_3_prior_dense_rank",
        "last_known_premium_distance_avg_3_prior",
        "premium_distance_avg_3_prior_percent_rank",
        "premium_distance_avg_3_prior_vs_second_start_dat",
        "second_start_date_month_avg_premium_distance_sum",
    )
    .orderBy(F.col("product_end_date_month_max_premium").desc(), "id")
    .limit(10)
)
