from pyspark.sql import functions as F

valid_store_sales = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 5]) & F.col("product_id").isin([4, 6, 7, 8]))
)

summary = (
    valid_store_sales
    .groupBy("store_id")
    .agg(F.round(F.stddev("revenue"), 2).alias("stddev_revenue"))
)

result = summary.filter(F.col("stddev_revenue") > 77)
