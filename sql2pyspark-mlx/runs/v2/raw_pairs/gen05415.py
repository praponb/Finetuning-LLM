from pyspark.sql import functions as F

base = df_order_items.filter(F.col("product_id").isNull() & (F.col("quantity") >= 6))

summary = (
    base
    .groupBy("order_id")
    .agg(
        F.max("quantity").alias("max_quantity"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
)

result = summary.orderBy(F.col("max_quantity").desc())
