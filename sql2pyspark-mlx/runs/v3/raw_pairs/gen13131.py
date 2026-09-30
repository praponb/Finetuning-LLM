from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") > 230)
    .groupBy(
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 2, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg((F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"))
    .filter(F.col("unit_price_range") > 33)
    .orderBy("quantity_bucket")
    .limit(5)
)
