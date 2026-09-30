from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("greatest_revenue_units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id", "product_id").orderBy("running_sum_greatest_revenue_units", "id")
w_hist = Window.orderBy(F.col("running_avg_greatest_revenue_units").desc_nulls_first())
w4 = Window.orderBy(F.col("running_sum_greatest_revenue_units").desc_nulls_last())
w_grp = Window.orderBy("running_sum_greatest_revenue_units_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 2, 4, 6]))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        F.greatest("revenue", "units").alias("greatest_revenue_units"),
        F.last_day("sale_date").alias("sale_date_month_end"),
    )
)

with_windows = (
    filtered
    .select(
        "*",
        F.min("greatest_revenue_units").over(w).alias("running_min_greatest_revenue_units"),
        F.avg("greatest_revenue_units").over(w).alias("running_avg_greatest_revenue_units"),
        F.sum("greatest_revenue_units").over(w).alias("running_sum_greatest_revenue_units"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "running_avg_greatest_revenue_units",
        "running_sum_greatest_revenue_units",
        F.lead("units").over(w2).alias("next_units"),
        F.lag("running_avg_greatest_revenue_units").over(w2).alias("prev_running_avg_greatest_revenue_units"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

metrics = (
    scored
    .filter(F.col("prev_running_avg_greatest_revenue_units") < 22)
    .select(
        "*",
        F.cume_dist().over(w_hist).alias("running_avg_greatest_revenue_units_cume_dist"),
        F.percent_rank().over(w4).alias("running_sum_greatest_revenue_units_percent_rank"),
    )
)

with_windows_store_sales = (
    metrics
    .select(
        "*",
        F.first("running_sum_greatest_revenue_units_percent_rank", ignorenulls=True).over(w_grp).alias("first_known_running_sum_greatest_revenue_units_percent_rank"),
        F.sum("prev_running_avg_greatest_revenue_units").over(w_grp).alias("cumulative_prev_running_avg_greatest_revenue_units"),
    )
)

valid = (
    with_windows_store_sales
    .filter(F.col("running_avg_greatest_revenue_units_cume_dist") > 0.54)
    .withColumn("seq_no_str", F.col("seq_no").cast("string"))
)

result = (
    valid
    .select(
        "id",
        "running_avg_greatest_revenue_units",
        "running_sum_greatest_revenue_units",
        "next_units",
        "prev_running_avg_greatest_revenue_units",
        "first_known_running_sum_greatest_revenue_units_percent_rank",
        "cumulative_prev_running_avg_greatest_revenue_units",
        "seq_no_str",
    )
)
