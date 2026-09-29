from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") >= 120)
    .rollup(
        "product_id",
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 2, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg((F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"))
    .orderBy("quantity_bucket")
)
