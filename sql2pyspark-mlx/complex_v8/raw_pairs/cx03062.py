from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_start_date_month_end_date_mo = Window.partitionBy(F.month("start_date"), F.date_trunc("month", "end_date")).orderBy(F.col("start_date").desc())
w_product_status_by_start_date = Window.partitionBy("product", "status").orderBy("start_date", "id")
w_product = Window.partitionBy("product")
w_end_date_month_by_start_date = Window.partitionBy(F.month("end_date")).orderBy("start_date", "id").rowsBetween(-3, Window.currentRow)
w_start_date_month_end_date_mo_2 = Window.partitionBy(F.month("start_date"), F.date_trunc("month", "end_date")).orderBy(F.col("start_date").desc(), "id").rowsBetween(-2, Window.currentRow)
w_end_date_month_by_start_date_2 = Window.partitionBy(F.month("end_date")).orderBy("start_date", "id")
w_start_date_month_end_date_mo_3 = Window.partitionBy(F.month("start_date"), F.date_trunc("month", "end_date")).orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_second_end_date_year = Window.partitionBy("status", F.year("second_end_date")).orderBy("start_date_rank")
w_end_date_month = Window.partitionBy(F.month("end_date"))
w_status_second_end_date_year_2 = Window.partitionBy("status", F.year("second_end_date")).orderBy("start_date_rank", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped_policys = (
    df_policies
    .filter((F.col("holder_id").isNotNull()) & (F.col("start_date").isNotNull()))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
    )
)

with_trends = (
    scoped_policys
    .select(
        "id",
        "product",
        "start_date",
        "end_date",
        "status",
        F.dense_rank().over(w_start_date_month_end_date_mo).alias("start_date_dense_rank"),
        F.rank().over(w_product_status_by_start_date).alias("start_date_rank"),
        F.round(F.col("premium") - F.avg("premium").over(w_product), 2).alias("premium_vs_product_avg"),
        F.round(F.avg("premium").over(w_end_date_month_by_start_date), 2).alias("premium_avg_4"),
        F.max("premium").over(w_start_date_month_end_date_mo_2).alias("premium_max_3"),
        F.lag("start_date", 3).over(w_end_date_month_by_start_date_2).alias("prev3_start_date"),
        F.min("premium").over(w_product).alias("product_min_premium"),
        F.nth_value("end_date", 2).over(w_start_date_month_end_date_mo_3).alias("second_end_date"),
        F.size(F.collect_set("status").over(w_product)).alias("distinct_status_in_product"),
    )
)

windowed = (
    with_trends
    .select(
        "*",
        F.round(F.cume_dist().over(w_status_second_end_date_year), 3).alias("start_date_rank_cume_dist"),
        F.round(F.col("start_date_dense_rank") / F.nullif(F.sum("start_date_dense_rank").over(w_end_date_month), F.lit(0)), 4).alias("start_date_dense_rank_share_of_end_date_month"),
        F.size(F.collect_set("product").over(w_end_date_month)).alias("distinct_product_in_end_date_month"),
        F.round(F.avg("distinct_status_in_product").over(w_end_date_month), 2).alias("end_date_month_avg_distinct_status_in_product"),
        F.round(F.col("premium_vs_product_avg") / F.nullif(F.sum("premium_vs_product_avg").over(w_end_date_month), F.lit(0)), 4).alias("premium_vs_product_avg_share_of_end_date_month"),
        F.sum("premium_avg_4").over(w_end_date_month).alias("end_date_month_sum_premium_avg_4"),
        F.size(F.collect_set("status").over(w_end_date_month)).alias("distinct_status_in_end_date_month"),
        F.nth_value("premium_max_3", 2).over(w_status_second_end_date_year_2).alias("second_premium_max_3"),
    )
)

result = (
    windowed
    .select(
        "id",
        "product",
        "start_date",
        "status",
        "start_date_dense_rank",
        "start_date_rank",
        "premium_avg_4",
        "premium_max_3",
        "prev3_start_date",
        "product_min_premium",
        "distinct_status_in_product",
        "start_date_rank_cume_dist",
        "start_date_dense_rank_share_of_end_date_month",
        "distinct_product_in_end_date_month",
        "end_date_month_avg_distinct_status_in_product",
        "end_date_month_sum_premium_avg_4",
        "distinct_status_in_end_date_month",
        F.length(F.trim(F.col("product"))).alias("product_len"),
    )
    .orderBy(
        F.col("distinct_product_in_end_date_month").asc_nulls_last(),
        F.col("premium_max_3").desc(),
    )
)
