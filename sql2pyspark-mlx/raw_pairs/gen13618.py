from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.max("last_restock").alias("latest_last_restock"),
    )
)
