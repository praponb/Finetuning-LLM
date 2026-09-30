from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "left")
    .groupBy(
        F.when(F.col("products.price") >= 1300, "high").when(F.col("products.price") >= 480, "mid").otherwise("low").alias("price_bucket"),
    )
    .agg(
        F.avg(F.col("order_items.unit_price")).alias("avg_unit_price"),
        F.max(F.col("order_items.unit_price")).alias("max_unit_price"),
    )
)
