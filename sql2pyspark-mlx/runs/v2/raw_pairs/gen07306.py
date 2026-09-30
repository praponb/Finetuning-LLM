from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id", F.month("last_restock").alias("last_restock_month"))
    .agg(F.count("quantity").alias("quantity_count"))
    .filter(F.col("quantity_count") < 9)
    .select("product_id", "last_restock_month", "quantity_count")
    .orderBy(F.col("product_id").desc())
)
