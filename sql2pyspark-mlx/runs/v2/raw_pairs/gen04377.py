from pyspark.sql import functions as F

result = (
    df_order_items
    .agg(
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
)
