from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 2, 4, 5]))
    .groupBy("store_id", "product_id")
    .agg(
        F.max("sale_date").alias("last_sale_date"),
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
    )
    .orderBy("store_id")
)
