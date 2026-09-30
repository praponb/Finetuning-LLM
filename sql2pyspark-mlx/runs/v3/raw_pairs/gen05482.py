from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("quantity") >= 9, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
        F.min("quantity").alias("min_quantity"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
    .orderBy(F.col("min_quantity").desc())
)
