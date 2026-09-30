from pyspark.sql import functions as F

result = (
    df_order_items
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
    )
    .orderBy(F.col("max_quantity").desc())
)
