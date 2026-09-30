from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") <= 350) & ((F.col("quantity") <= 6) | (F.col("unit_price") >= 210)))
    .groupBy("order_id")
    .agg(
        F.avg("unit_price").alias("avg_unit_price"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
    .orderBy("avg_unit_price")
)
