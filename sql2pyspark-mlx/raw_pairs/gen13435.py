from pyspark.sql import functions as F

valid_order_items = (
    df_order_items
    .filter(F.col("order_id").isin([3, 20, 28, 30]))
    .withColumn("quantity_rounded", F.round("quantity", 2))
)

agg = (
    valid_order_items
    .groupBy("order_id")
    .agg(
        F.avg("unit_price").alias("avg_unit_price"),
        F.count("*").alias("row_count"),
    )
)

result = agg.select("order_id", "avg_unit_price", "row_count")
