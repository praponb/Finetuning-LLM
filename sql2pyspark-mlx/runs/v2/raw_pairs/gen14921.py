from pyspark.sql import functions as F

base = df_store_sales.filter(F.col("product_id").isin([2, 4, 6, 9]))

store_sales_stats = (
    base
    .groupBy("product_id")
    .agg(
        F.round(F.stddev("units"), 2).alias("stddev_units"),
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
    )
)

result = (
    store_sales_stats
    .filter(F.col("stddev_units") >= 80)
    .orderBy(F.col("product_id").desc())
)
