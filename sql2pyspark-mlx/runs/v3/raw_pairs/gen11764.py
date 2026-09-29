from pyspark.sql import functions as F

base = df_order_items.filter(F.col("product_id").isNotNull())

summary = (
    base
    .groupBy(
        F.when(F.col("unit_price") >= 350, "high").when(F.col("unit_price") >= 110, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
    )
)

result = (
    summary
    .filter(F.col("revenue") > 37)
    .select("unit_price_bucket", "revenue")
)
