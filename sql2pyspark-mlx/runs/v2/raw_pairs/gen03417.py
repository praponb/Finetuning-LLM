from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "left")
    .filter(F.col("orders.channel").isin(["partner", "mobile", "web"]))
    .groupBy(F.col("orders.channel"))
    .agg(
        F.sum((F.col("order_items.unit_price") * F.col("order_items.quantity"))).alias("revenue"),
        F.sum(F.col("order_items.unit_price")).alias("total_unit_price"),
    )
    .orderBy("total_unit_price", "channel")
    .limit(5)
)
