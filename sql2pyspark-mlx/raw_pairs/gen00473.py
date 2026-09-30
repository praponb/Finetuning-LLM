from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id")
    .agg(F.min("last_restock").alias("first_last_restock"))
    .orderBy("product_id")
)
