from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("store_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_store_sales
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        F.count("revenue").over(w_part).alias("running_count_revenue"),
        F.max("revenue").over(w_part).alias("running_max_revenue"),
        F.sum("revenue").over(w_part).alias("running_sum_revenue"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("running_count_revenue") >= 18, "LARGE").otherwise("SMALL").alias("running_count_revenue_tier"),
    )
    .agg(
        F.round(F.avg("running_count_revenue"), 2).alias("avg_running_count_revenue"),
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.percentile_approx("running_count_revenue", 0.9).alias("running_count_revenue_p90"),
    )
)
