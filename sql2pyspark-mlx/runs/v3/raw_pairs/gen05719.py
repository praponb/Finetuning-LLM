from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([8, 14, 16, 38]))
    .groupBy(
        F.when(F.col("unit_price") >= 360, "high").when(F.col("unit_price") >= 190, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.sum("unit_price").alias("total_unit_price"),
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
    )
    .filter(F.col("revenue") < 77)
    .orderBy("revenue")
)
