from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") < 4)
    .groupBy(
        F.when(F.col("unit_price") >= 420, "high").when(F.col("unit_price") >= 100, "mid").otherwise("low").alias("unit_price_bucket"),
        "product_id",
    )
    .agg(
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
        F.max("unit_price").alias("max_unit_price"),
    )
    .filter(F.col("max_unit_price") < 300)
    .select("unit_price_bucket", "product_id", "unit_price_range", "max_unit_price")
    .orderBy("unit_price_bucket", "product_id")
    .limit(5)
)
