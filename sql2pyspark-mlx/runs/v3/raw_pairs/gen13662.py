from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(~(F.col("unit_price").between(340, 390) & (F.col("product_id") == 16)))
    .groupBy("product_id")
    .agg(
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
    .orderBy(F.col("revenue").desc())
)
