from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(~(F.col("order_id").isin([30, 38]) & (F.col("product_id") == 3)))
    .groupBy(
        F.when(F.col("unit_price") >= 380, "high").when(F.col("unit_price") >= 130, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.min("unit_price").alias("min_unit_price"),
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
    )
    .orderBy("unit_price_bucket")
)
