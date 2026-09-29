from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") <= 5)
    .groupBy(
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"))
    .filter(F.col("revenue") >= 65)
    .select("quantity_bucket", "revenue")
    .orderBy("quantity_bucket")
    .limit(5)
)
