from pyspark.sql import functions as F

src = (
    df_order_items
    .filter(F.col("product_id").isNull() & F.col("order_id").isin([6, 25, 27, 40]))
)

order_items_stats = (
    src
    .groupBy(
        F.when(F.col("unit_price") >= 430, "high").when(F.col("unit_price") >= 60, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
        F.sum("quantity").alias("total_quantity"),
    )
)

result = order_items_stats.orderBy("total_quantity")
