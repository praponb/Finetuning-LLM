from pyspark.sql import functions as F

result = (
    df_order_items
    .select("unit_price")
    .subtract(df_orders.select(F.col("amount").alias("unit_price")))
)
