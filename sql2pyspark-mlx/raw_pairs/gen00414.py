from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") <= 380)
    .groupBy(
        F.when(F.col("unit_price") >= 420, "high").when(F.col("unit_price") >= 120, "mid").otherwise("low").alias("unit_price_bucket"),
        "product_id",
    )
    .agg(
        F.count("*").alias("num_order_items"),
        F.sum("quantity").alias("total_quantity"),
    )
    .orderBy(F.col("total_quantity").desc())
)
