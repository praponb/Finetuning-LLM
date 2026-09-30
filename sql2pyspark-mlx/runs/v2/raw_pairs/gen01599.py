from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 4, "mid").otherwise("low").alias("quantity_bucket"),
        "order_id",
    )
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.min("unit_price").alias("min_unit_price"),
    )
    .filter(F.col("avg_quantity") >= 4.9)
    .select("quantity_bucket", "order_id", "avg_quantity", "min_unit_price")
    .orderBy(F.col("order_id").desc())
)
