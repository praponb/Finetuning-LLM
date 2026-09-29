from pyspark.sql import functions as F

result = (
    df_order_items
    .agg(
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
        F.sum("quantity").alias("total_quantity"),
        F.avg("unit_price").alias("avg_unit_price"),
    )
)
