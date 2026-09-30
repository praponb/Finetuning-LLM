from pyspark.sql import functions as F

base = df_order_items.filter(F.col("product_id").isNotNull())

order_items_stats = (
    base
    .groupBy(
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 4, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.min("unit_price").alias("min_unit_price"),
    )
)

result = (
    order_items_stats
    .select("quantity_bucket", "total_quantity", "min_unit_price")
    .orderBy(F.col("total_quantity").desc(), F.col("quantity_bucket").desc())
    .limit(10)
)
