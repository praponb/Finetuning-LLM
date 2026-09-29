from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").between(130, 210))
    .groupBy(
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 2, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg((F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"))
    .filter(F.col("unit_price_range") < 71)
    .orderBy(F.col("unit_price_range").desc())
)
