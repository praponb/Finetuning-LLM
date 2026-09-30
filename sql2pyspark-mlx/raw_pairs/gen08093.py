from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee").isNotNull())
    .select("shipping_fee", F.lit("order").alias("kind"))
    .union(
        df_order_items
        .filter(F.col("unit_price").isNotNull())
        .select("unit_price", F.lit("order_item").alias("kind"))
    )
)
