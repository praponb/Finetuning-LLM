from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_start_date_full = Window.orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_start_date_year = Window.partitionBy(F.year("start_date"))
w_all_by_end_date_running = Window.orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_start_date_month = Window.partitionBy(F.month("start_date"))
w_start_date_dayofweek_product = Window.partitionBy("start_date_dayofweek", "product").orderBy("start_date")
w_start_date_dayofweek_product_2 = Window.partitionBy("start_date_dayofweek", "product").orderBy("start_date", "id")
w_all_by_start_date_running = Window.orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_start_date = Window.orderBy(F.col("start_date").desc(), "id")
w_all_by_end_date = Window.orderBy("end_date", "id")
w_product_start_date_dayofweek = Window.partitionBy("product", "start_date_dayofweek").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_product_or_unknown = Window.partitionBy("product_or_unknown")
w_status_start_date_month_by_s = Window.partitionBy("status", F.month("start_date")).orderBy("start_date_year_avg_premium", "id")
w_status_start_date_month_by_s_2 = Window.partitionBy("status", F.month("start_date")).orderBy("start_date_year_avg_premium", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned_policys = (
    df_policies
    .filter((F.col("end_date") < F.lit("2026-07-11").cast("date")) | (~F.col("product").isin("TRAVEL", "HEALTH")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.dayofweek(F.col("start_date")).alias("start_date_dayofweek"),
    )
)

sequenced = (
    cleaned_policys
    .select(
        "*",
        F.nth_value("start_date", 2).over(w_all_by_start_date_full).alias("second_start_date"),
        F.round(F.col("premium") - F.avg("premium").over(w_start_date_year), 2).alias("premium_vs_start_date_year_avg"),
        F.first("premium", ignorenulls=True).over(w_all_by_end_date_running).alias("first_known_premium"),
        F.count(F.lit(1)).over(w_start_date_year).alias("rows_in_start_date_year"),
        F.sum("premium").over(w_start_date_month).alias("start_date_month_sum_premium"),
        F.rank().over(w_start_date_dayofweek_product).alias("start_date_rank"),
        F.lag("start_date").over(w_start_date_dayofweek_product_2).alias("prev_start_date"),
        F.sum("start_date_dayofweek").over(w_all_by_start_date_running).alias("running_start_date_dayofweek"),
        F.row_number().over(w_all_by_start_date).alias("seq_no"),
        F.lag("start_date", 3).over(w_all_by_end_date).alias("prev3_start_date"),
        F.ntile(3).over(w_all_by_end_date).alias("end_date_tercile"),
        F.round(F.avg("premium").over(w_start_date_year), 2).alias("start_date_year_avg_premium"),
    )
)

scored = (
    sequenced
    .filter(F.col("start_date_dayofweek") > 3)
    .withColumn("product_or_unknown", F.coalesce(F.col("product"), F.lit("UNKNOWN")))
)

windowed = (
    scored
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "second_start_date",
        "premium_vs_start_date_year_avg",
        "first_known_premium",
        "start_date_month_sum_premium",
        "start_date_rank",
        "prev_start_date",
        "running_start_date_dayofweek",
        "seq_no",
        "prev3_start_date",
        "end_date_tercile",
        "start_date_year_avg_premium",
        "product_or_unknown",
        F.count(F.lit(1)).over(w_product_start_date_dayofweek).alias("running_count"),
        F.sum("first_known_premium").over(w_product_or_unknown).alias("product_or_unknown_sum_first_known_premium"),
        F.lag("first_known_premium").over(w_status_start_date_month_by_s).alias("prev_first_known_premium"),
        F.max("rows_in_start_date_year").over(w_product_or_unknown).alias("product_or_unknown_max_rows_in_start_date_year"),
        F.nth_value("rows_in_start_date_year", 2).over(w_status_start_date_month_by_s_2).alias("second_rows_in_start_date_year"),
    )
)

result = (
    windowed
    .filter(F.col("product_or_unknown") == "HEALTH")
    .select(
        "id",
        "holder_id",
        "product",
        "second_start_date",
        "premium_vs_start_date_year_avg",
        "first_known_premium",
        "start_date_month_sum_premium",
        "running_start_date_dayofweek",
        "prev3_start_date",
        "end_date_tercile",
        "product_or_unknown",
        "product_or_unknown_sum_first_known_premium",
        "prev_first_known_premium",
        "product_or_unknown_max_rows_in_start_date_year",
        "second_rows_in_start_date_year",
        F.initcap(F.trim(F.col("product_or_unknown"))).alias("product_or_unknown_clean"),
        F.concat_ws("|", F.col("product_or_unknown"), F.col("product")).alias("product_or_unknown_product_label"),
    )
)
