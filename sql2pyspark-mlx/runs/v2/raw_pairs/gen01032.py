from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.filter(~F.col("channel").isin(["store", "partner", "web"])).alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "left_semi")
    .select(F.col("order_items.id"), F.col("order_items.product_id"))
)
