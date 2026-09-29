from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy(F.col("units").desc(), "id")
w3 = Window.orderBy("running_sum_revenue", "id").rowsBetween(-3, -1)
w4 = Window.orderBy("running_sum_revenue", "id")
w_recent = Window.orderBy("running_sum_revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("running_min_revenue_avg_3_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("running_count_prev_running_min_revenue", "id").rowsBetween(-6, Window.currentRow)

enriched = (
    df_store_sales
    .select(
        "id",
        "units",
        F.avg("revenue").over(w).alias("running_avg_revenue"),
        F.min("revenue").over(w).alias("running_min_revenue"),
        F.sum("revenue").over(w).alias("running_sum_revenue"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

enriched_store_sales = (
    enriched
    .filter(F.col("units").between(15, 20))
    .select(
        "id",
        "row_seq",
        F.avg("running_min_revenue").over(w3).alias("running_min_revenue_avg_3_prior"),
        F.lag("running_min_revenue", 2).over(w4).alias("prev_running_min_revenue"),
        F.first("running_min_revenue", ignorenulls=True).over(w_recent).alias("first_known_running_min_revenue"),
    )
)

scored = (
    enriched_store_sales
    .select(
        "id",
        "prev_running_min_revenue",
        "first_known_running_min_revenue",
        F.sum("prev_running_min_revenue").over(w6).alias("running_sum_prev_running_min_revenue"),
        F.max("prev_running_min_revenue").over(w6).alias("running_max_prev_running_min_revenue"),
        F.count("prev_running_min_revenue").over(w6).alias("running_count_prev_running_min_revenue"),
    )
)

with_windows = (
    scored
    .select(
        "id",
        "prev_running_min_revenue",
        "first_known_running_min_revenue",
        "running_max_prev_running_min_revenue",
        F.min("running_count_prev_running_min_revenue").over(w7).alias("last7_min_running_count_prev_running_min_revenue"),
        F.sum("running_count_prev_running_min_revenue").over(w7).alias("last7_sum_running_count_prev_running_min_revenue"),
        F.avg("running_count_prev_running_min_revenue").over(w7).alias("last7_avg_running_count_prev_running_min_revenue"),
        F.max("running_count_prev_running_min_revenue").over(w7).alias("last7_max_running_count_prev_running_min_revenue"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "prev_running_min_revenue",
        "last7_sum_running_count_prev_running_min_revenue",
        "last7_avg_running_count_prev_running_min_revenue",
        "last7_max_running_count_prev_running_min_revenue",
    )
    .orderBy(
        F.col("last7_avg_running_count_prev_running_min_revenue").desc_nulls_first(),
        F.col("prev_running_min_revenue").desc(),
    )
)
