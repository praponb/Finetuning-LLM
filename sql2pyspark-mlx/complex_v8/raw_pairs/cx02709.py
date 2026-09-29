from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_start_date_year_month_by_pre = Window.partitionBy(F.month("start_date_year")).orderBy(F.col("premium").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_end_date_ties = Window.orderBy("end_date")
w_product_holder_id = Window.partitionBy("product", "holder_id")
w_holder_id_status = Window.partitionBy("holder_id", "status")
w_all_by_end_date_running = Window.orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_premium_moving_3 = Window.orderBy("premium", "id").rowsBetween(-3, Window.currentRow)

prepared_policys = (
    df_policies
    .filter((F.col("start_date").isNotNull()) & (F.col("product") != "LIFE"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.concat(F.col("status"), F.lit("#"), F.col("id").cast("string")).alias("status_ref"),
        F.date_trunc("year", F.col("start_date")).alias("start_date_year"),
    )
)

with_windows = (
    prepared_policys
    .select(
        "*",
        F.nth_value("start_date_year", 2).over(w_start_date_year_month_by_pre).alias("second_start_date_year"),
        F.round(F.percent_rank().over(w_all_by_end_date_ties), 3).alias("end_date_percent_rank"),
        F.round(F.col("premium") - F.avg("premium").over(w_product_holder_id), 2).alias("premium_vs_product_holder_id_avg"),
        F.count(F.lit(1)).over(w_product_holder_id).alias("rows_in_product_holder_id"),
        F.size(F.collect_set("product").over(w_holder_id_status)).alias("distinct_product_in_holder_id_status"),
        F.sum("premium").over(w_all_by_end_date_running).alias("running_premium"),
        F.min("premium").over(w_product_holder_id).alias("product_holder_id_min_premium"),
        F.sum("premium").over(w_all_by_premium_moving_3).alias("premium_sum_4"),
        F.round(F.avg("premium").over(w_product_holder_id), 2).alias("product_holder_id_avg_premium"),
        F.size(F.collect_set("status").over(w_product_holder_id)).alias("distinct_status_in_product_holder_id"),
    )
)

result = (
    with_windows
    .filter(F.month(F.col("start_date_year")).isin(2, 3))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
        "start_date_year",
        "second_start_date_year",
        "end_date_percent_rank",
        "rows_in_product_holder_id",
        "distinct_product_in_holder_id_status",
        "product_holder_id_min_premium",
        "product_holder_id_avg_premium",
        F.to_date(F.col("start_date_year")).alias("start_date_year_date"),
    )
    .orderBy(F.col("status").desc(), F.col("end_date_percent_rank").desc())
)
