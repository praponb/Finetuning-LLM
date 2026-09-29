from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .filter(F.col("oi.product_id").isin([3, 6]))
    .groupBy(
        F.when(F.col("p.cost") >= 540, "high").when(F.col("p.cost") >= 200, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(F.countDistinct(F.col("oi.quantity")).alias("unique_orders"))
)
