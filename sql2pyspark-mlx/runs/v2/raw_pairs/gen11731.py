from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "outer")
    .groupBy(
        F.when(F.col("p.cost") >= 670, "high").when(F.col("p.cost") >= 320, "mid").otherwise("low").alias("cost_bucket"),
        F.month(F.col("p.launch_date")).alias("launch_month"),
    )
    .agg(F.sum((F.col("oi.unit_price") * F.col("oi.quantity"))).alias("revenue"))
)
