from pyspark.sql import functions as F

result = (
    df_order_items
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "left")
    .select(
        "a.id",
        "a.order_id",
        "a.product_id",
        F.col("d.shipping_fee"),
        F.col("d.status"),
        F.col("d.discount"),
    )
)
