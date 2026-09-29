from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.filter(F.col("channel").isin(["web", "partner"])).alias("o"), F.col("oi.order_id") == F.col("o.id"), "left_anti")
    .select(F.col("oi.id"), F.col("oi.order_id"), F.col("oi.product_id"))
    .orderBy(F.col("order_id").desc(), "id")
)
