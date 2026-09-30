from pyspark.sql import functions as F

base = (
    df_order_items
    .filter(F.col("product_id").isNull() & (F.col("product_id") == 19))
    .withColumn("line_total", (F.col("unit_price") * F.col("quantity")))
)

order_items_stats = (
    base
    .groupBy(
        F.when(F.col("quantity") >= 9, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        (F.max("line_total") - F.min("line_total")).alias("line_total_range"),
        F.max("unit_price").alias("max_unit_price"),
    )
)

result = (
    order_items_stats
    .select("quantity_bucket", "line_total_range", "max_unit_price")
    .orderBy("line_total_range", "quantity_bucket")
    .limit(10)
)
