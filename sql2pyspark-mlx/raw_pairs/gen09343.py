from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 300, "high").when(F.col("unit_price") >= 150, "mid").otherwise("low").alias("unit_price_bucket"),
        "product_id",
    )
    .agg(F.max("quantity").alias("max_quantity"))
    .orderBy(F.col("max_quantity").desc(), "unit_price_bucket", "product_id")
    .limit(10)
)
