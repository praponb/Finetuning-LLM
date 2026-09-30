from pyspark.sql import functions as F

filtered_order_items = df_order_items.filter(F.col("quantity").between(6, 7))

order_id_summary = (
    filtered_order_items
    .groupBy("order_id")
    .agg(
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
        F.avg("quantity").alias("avg_quantity"),
    )
)

result = (
    order_id_summary
    .orderBy(F.col("revenue").desc(), "order_id")
    .limit(10)
)
