from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        "order_id",
        "product_id",
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
    )
    .filter(F.col("avg_unit_price") > 33)
    .orderBy("revenue", "order_id", "product_id", "quantity_bucket")
    .limit(3)
)
