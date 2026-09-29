from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") > 340)
    .select("unit_price")
    .unionByName(df_orders.select(F.col("amount").alias("unit_price")))
    .orderBy("unit_price")
)
