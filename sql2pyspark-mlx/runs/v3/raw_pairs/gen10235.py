from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id")
    .agg(F.round(F.avg("unit_price"), 2).alias("avg_unit_price"))
)
