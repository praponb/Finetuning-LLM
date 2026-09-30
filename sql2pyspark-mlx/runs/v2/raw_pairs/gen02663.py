from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        "product_id",
        F.when(F.col("quantity") >= 6, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.count("unit_price").alias("unit_price_count"))
    .filter(F.col("unit_price_count") >= 5)
    .orderBy(F.col("product_id").desc(), F.col("unit_price_count").desc())
)
