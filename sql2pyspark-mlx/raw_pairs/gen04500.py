from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") > 210)
    .groupBy(
        F.when(F.col("unit_price") >= 370, "high").when(F.col("unit_price") >= 140, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
)
