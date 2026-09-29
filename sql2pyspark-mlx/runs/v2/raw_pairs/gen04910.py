from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("product_id") == 3) & (F.col("quantity") >= 119))
    .groupBy("store_id")
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.sum("quantity").alias("total_quantity"),
    )
    .orderBy("total_quantity")
)
