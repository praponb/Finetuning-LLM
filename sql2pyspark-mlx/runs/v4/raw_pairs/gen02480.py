from pyspark.sql import functions as F

result = (
    df_orders
    .select("order_date")
    .subtract(df_inventory.select(F.col("last_restock").alias("order_date")))
)
