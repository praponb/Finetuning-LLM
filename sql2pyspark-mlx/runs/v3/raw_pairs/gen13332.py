from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") <= 212)
    .groupBy("product_id")
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.count("quantity").alias("quantity_count"),
    )
)
