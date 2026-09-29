from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.countDistinct("quantity").alias("unique_products"),
        F.max("quantity").alias("max_quantity"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") < 3)
    .drop("row_count")
)
