from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([2, 3, 5]))
    .groupBy("product_id")
    .agg(
        F.max("last_restock").alias("latest_last_restock"),
        F.max("quantity").alias("max_quantity"),
    )
    .filter(F.col("max_quantity") >= 130)
    .select("product_id", "latest_last_restock", "max_quantity")
    .orderBy("latest_last_restock")
)
