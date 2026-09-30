from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "left")
    .groupBy(
        F.col("o.channel"),
        F.when(F.col("o.amount") >= 1560, "high").when(F.col("o.amount") >= 350, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.countDistinct(F.col("oi.unit_price")).alias("unique_orders"))
    .orderBy("unique_orders")
)
