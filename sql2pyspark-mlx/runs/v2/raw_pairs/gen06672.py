from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
)
