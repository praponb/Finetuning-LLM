from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id") == 20)
    .groupBy("order_id")
    .agg(
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
    )
)
