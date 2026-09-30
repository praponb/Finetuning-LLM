from pyspark.sql import functions as F

result = (
    df_orders
    .alias("s")
    .join(df_shipments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.shipped_date") >= F.col("s.order_date")), "left_semi")
)
