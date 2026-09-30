from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.filter(~F.col("channel").isin(["store", "mobile", "web"])).alias("o"), F.col("oi.order_id") == F.col("o.id"), "left_anti")
    .select(
        F.col("oi.id"),
        F.col("oi.order_id"),
        F.col("oi.product_id"),
        F.col("oi.quantity"),
    )
    .orderBy("product_id", "id")
    .limit(10)
)
