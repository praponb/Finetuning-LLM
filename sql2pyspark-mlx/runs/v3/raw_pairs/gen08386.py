from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") == 67)
    .groupBy("store_id", "product_id")
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.max("quantity").alias("max_quantity"),
    )
    .filter(F.col("min_quantity") >= 180)
    .orderBy("max_quantity", "store_id", "product_id")
    .limit(5)
)
