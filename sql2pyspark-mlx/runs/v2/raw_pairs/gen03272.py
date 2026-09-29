from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.sum("unit_price").alias("total_unit_price"),
        F.avg("unit_price").alias("avg_unit_price"),
    )
    .filter(F.col("avg_unit_price") >= 150)
)
