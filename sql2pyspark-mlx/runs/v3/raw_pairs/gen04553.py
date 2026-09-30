from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([2, 29, 34]))
    .groupBy(
        F.when(F.col("unit_price") >= 350, "high").when(F.col("unit_price") >= 110, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
        F.countDistinct("unit_price").alias("unique_products"),
    )
    .filter(F.col("unit_price_range") < 26)
    .select("unit_price_bucket", "unit_price_range", "unique_products")
)
