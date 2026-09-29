from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").isNotNull() | F.col("quantity").isNotNull())
    .groupBy("order_id")
    .agg(
        F.avg("unit_price").alias("avg_unit_price"),
        F.sum("unit_price").alias("total_unit_price"),
    )
)
