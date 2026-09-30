from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.sum("unit_price").alias("total_unit_price"),
        F.avg("quantity").alias("avg_quantity"),
    )
    .filter(F.col("avg_quantity") >= 3.5)
)
