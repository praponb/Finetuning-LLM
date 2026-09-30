from pyspark.sql import functions as F

result = (
    df_order_items
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
        F.avg("unit_price").alias("avg_unit_price"),
    )
    .orderBy(F.col("avg_unit_price").desc(), F.col("revenue").desc())
)
