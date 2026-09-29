from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        "order_id",
        F.when(F.col("unit_price") >= 360, "high").when(F.col("unit_price") >= 130, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"))
    .filter(F.col("revenue") < 30)
    .select("order_id", "unit_price_bucket", "revenue")
    .orderBy("revenue", "order_id", "unit_price_bucket")
    .limit(10)
)
