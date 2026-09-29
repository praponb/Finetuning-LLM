from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") > 350)
    .groupBy(
        "product_id",
        F.when(F.col("unit_price") >= 400, "high").when(F.col("unit_price") >= 90, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg((F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"))
)
