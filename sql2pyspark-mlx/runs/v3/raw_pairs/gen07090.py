from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.shipping_fee") > 23)
    .groupBy(F.col("o.status"), F.col("o.customer_id"))
    .agg(
        F.sum((F.col("oi.unit_price") * F.col("oi.quantity"))).alias("revenue"),
        F.min(F.col("oi.unit_price")).alias("min_unit_price"),
    )
    .orderBy(F.col("min_unit_price").desc())
)
