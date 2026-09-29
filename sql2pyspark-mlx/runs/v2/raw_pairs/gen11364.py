from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("o.id") == F.col("s.order_id"), "left_semi")
    .select(F.col("o.id"), F.col("o.discount"))
    .orderBy(F.col("discount").desc())
)
