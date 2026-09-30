from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("revenue") <= 2600) & F.month("sale_date").isin([1, 3, 7]))
    .groupBy("store_id", "product_id")
    .agg(F.round(F.stddev("revenue"), 2).alias("stddev_revenue"))
    .orderBy("store_id")
)
