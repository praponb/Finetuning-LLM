from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") < 7) & ((F.col("quantity") <= 3) | F.col("unit_price").isNotNull()))
    .groupBy(
        F.when(F.col("unit_price") >= 400, "high").when(F.col("unit_price") >= 170, "mid").otherwise("low").alias("unit_price_bucket"),
        "product_id",
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 2, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.min("unit_price").alias("min_unit_price"),
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
        F.count("quantity").alias("quantity_count"),
    )
    .orderBy(
        "quantity_bucket",
        F.col("unit_price_range").desc(),
        "unit_price_bucket",
        "product_id",
    )
    .limit(5)
)
