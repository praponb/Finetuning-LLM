from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"))
    .orderBy(F.col("revenue").desc())
)
