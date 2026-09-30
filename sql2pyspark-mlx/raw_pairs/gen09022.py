from pyspark.sql import functions as F

recent_order_items = (
    df_order_items
    .filter(F.col("unit_price").between(190, 260) & (F.col("product_id").isin([4, 13, 17, 31]) | F.col("order_id").isin([15, 22])))
)

order_items_stats = (
    recent_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 340, "high").when(F.col("unit_price") >= 170, "mid").otherwise("low").alias("unit_price_bucket"),
        "product_id",
    )
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.count("*").alias("total_count"),
    )
)

result = order_items_stats.filter(F.col("total_quantity") > 18)
