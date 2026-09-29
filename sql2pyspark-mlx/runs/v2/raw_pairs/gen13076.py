from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 2, 4, 6]))
    .groupBy("product_id")
    .agg(F.round(F.stddev("revenue"), 2).alias("stddev_revenue"))
    .orderBy("stddev_revenue")
)
