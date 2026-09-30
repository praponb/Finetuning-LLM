from pyspark.sql import functions as F

result = (
    df_orders
    .select("shipping_fee", F.lit("order").alias("source"))
    .union(
        df_order_items
        .filter(F.col("quantity") != 5)
        .select("unit_price", F.lit("order_item").alias("source"))
    )
    .distinct()
)
