from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").between(4, 6))
    .groupBy(
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
        F.when(F.col("unit_price") >= 370, "high").when(F.col("unit_price") >= 160, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
    .filter(F.col("quantity_range") > 66)
    .orderBy(F.col("unit_price_bucket").desc())
)
