from pyspark.sql import functions as F

base = (
    df_order_items
    .filter((F.col("quantity") != 5) & ((F.col("quantity") < 6) | F.col("unit_price").between(240, 390)))
)

summary = (
    base
    .groupBy("product_id")
    .agg(
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
        F.min("quantity").alias("min_quantity"),
    )
)

result = summary.orderBy(F.col("min_quantity").desc())
