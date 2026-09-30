from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").between(140, 390) & F.col("unit_price").between(110, 180) & F.col("product_id").isin([19, 33, 37, 38]))
    .groupBy(
        F.when(F.col("unit_price") >= 330, "high").when(F.col("unit_price") >= 90, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.countDistinct("unit_price").alias("unique_products"),
        F.min("unit_price").alias("min_unit_price"),
    )
    .filter(F.col("min_unit_price") >= 260)
    .drop("min_unit_price")
    .orderBy(F.col("unique_products").desc())
)
