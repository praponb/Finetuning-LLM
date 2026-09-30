from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.store_id").isNotNull())
    .groupBy(
        F.when(F.col("o.amount") >= 1790, "high").when(F.col("o.amount") >= 460, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.max(F.col("oi.quantity")).alias("max_quantity"))
)
