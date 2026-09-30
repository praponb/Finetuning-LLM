from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id", "store_id")
    .agg(F.min("last_restock").alias("first_last_restock"))
    .orderBy("first_last_restock", "product_id")
)
