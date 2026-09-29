from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .groupBy(
        F.when(F.col("o.amount") >= 1690, "high").when(F.col("o.amount") >= 760, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.countDistinct(F.col("oi.quantity")).alias("unique_orders"),
        F.count("*").alias("cnt"),
    )
    .orderBy("amount_bucket")
)
