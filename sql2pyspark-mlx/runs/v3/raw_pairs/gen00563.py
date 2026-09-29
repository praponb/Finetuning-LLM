from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([2, 20, 21]))
    .groupBy(
        F.when(F.col("unit_price") >= 340, "high").when(F.col("unit_price") >= 90, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg((F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"))
    .filter(F.col("unit_price_range") > 49)
    .select("unit_price_bucket", "unit_price_range")
    .orderBy("unit_price_range")
)
