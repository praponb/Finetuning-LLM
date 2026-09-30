from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty") >= 308)
    .select(F.col("launch_date").alias("event_date"))
    .unionByName(df_inventory.select(F.col("last_restock").alias("event_date")))
    .distinct()
)
