from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([18, 25, 28]))
    .select("unit_price")
    .unionByName(df_orders.select(F.col("shipping_fee").alias("unit_price")))
    .distinct()
)
