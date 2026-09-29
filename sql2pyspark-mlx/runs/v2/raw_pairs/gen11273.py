from pyspark.sql import functions as F

base = (
    df_order_items
    .filter(F.col("order_id").isin([16, 28, 31, 38]) & F.col("product_id").isNotNull())
    .withColumn("line_total", F.round((F.col("unit_price") / F.col("quantity")), 2))
)

order_items_stats = (
    base
    .groupBy(
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 2, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.min("quantity").alias("min_quantity"))
)

result = (
    order_items_stats
    .select("quantity_bucket", "min_quantity")
    .orderBy("quantity_bucket")
)
