from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") <= 240)
    .groupBy(
        F.when(F.col("unit_price") >= 370, "high").when(F.col("unit_price") >= 190, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
        F.count("*").alias("total_count"),
    )
    .filter(F.col("revenue") >= 28)
    .select("unit_price_bucket", "revenue", "total_count")
)
