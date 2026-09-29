from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel") == "mobile")
    .select("shipping_fee", F.lit("order").alias("source"))
    .unionByName(
        df_order_items
        .filter(F.col("quantity") != 4)
        .select(
        F.col("unit_price").alias("shipping_fee"),
        F.lit("order_item").alias("source"),
    )
    )
    .distinct()
)
