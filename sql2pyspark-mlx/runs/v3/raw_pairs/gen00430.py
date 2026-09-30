from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("store_id") == 3) & F.col("store_id").isin([1, 4]) & F.col("revenue").between(1100, 2300))
    .groupBy("store_id", "product_id")
    .agg(
        F.min("sale_date").alias("first_sale_date"),
        F.min("revenue").alias("min_revenue"),
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
    )
)
