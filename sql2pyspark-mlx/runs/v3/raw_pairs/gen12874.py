from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") <= "2023-10-23")
    .groupBy("product_id")
    .agg(F.min("last_restock").alias("first_last_restock"))
    .orderBy("product_id")
)
