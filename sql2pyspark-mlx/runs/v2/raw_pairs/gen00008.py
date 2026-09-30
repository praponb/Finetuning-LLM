from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .groupBy(
        F.when(F.col("p.cost") >= 640, "high").when(F.col("p.cost") >= 360, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(F.avg(F.col("oi.quantity")).alias("avg_quantity"))
)
