from pyspark.sql import functions as F

result = (
    df_orders
    .select("amount")
    .unionByName(df_order_items.select(F.col("unit_price").alias("amount")))
    .distinct()
    .orderBy("amount")
)
