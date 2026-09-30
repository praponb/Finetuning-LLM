from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.store_id").isNull())
    .groupBy(
        F.col("p.brand"),
        F.when(F.col("p.cost") >= 750, "high").when(F.col("p.cost") >= 140, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(F.max(F.col("oi.quantity")).alias("max_quantity"))
    .orderBy(F.col("cost_bucket").desc())
)
