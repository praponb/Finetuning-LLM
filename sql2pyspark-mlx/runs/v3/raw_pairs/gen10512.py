from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 2, 4, 6]) & F.col("store_id").isin([3, 4, 5, 6]))
    .groupBy("product_id")
    .agg(
        F.sum("units").alias("total_units"),
        F.countDistinct("revenue").alias("unique_products"),
    )
)
