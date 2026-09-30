from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").between(129, 175))
    .groupBy("product_id", "store_id")
    .agg(
        F.max("last_restock").alias("latest_last_restock"),
        F.count("quantity").alias("quantity_count"),
    )
    .filter(F.col("quantity_count") >= 4)
    .select("product_id", "store_id", "latest_last_restock", "quantity_count")
)
