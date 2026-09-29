from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        "product_id",
        F.when(F.col("unit_price") >= 420, "high").when(F.col("unit_price") >= 120, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(F.min("unit_price").alias("min_unit_price"))
    .orderBy(F.col("unit_price_bucket").desc())
)
