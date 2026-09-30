from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").between(6, 8))
    .groupBy(
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 2, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.min("unit_price").alias("min_unit_price"),
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
        F.count("quantity").alias("quantity_count"),
    )
    .filter(F.col("quantity_count") > 3)
    .drop("quantity_count")
)
