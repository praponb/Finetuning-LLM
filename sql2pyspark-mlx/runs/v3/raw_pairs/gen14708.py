from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id") == 34)
    .groupBy(
        F.when(F.col("unit_price") >= 310, "high").when(F.col("unit_price") >= 130, "mid").otherwise("low").alias("unit_price_bucket"),
        "product_id",
    )
    .agg(
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
        F.min("unit_price").alias("min_unit_price"),
    )
    .filter(F.col("min_unit_price") > 400)
    .orderBy("product_id")
)
