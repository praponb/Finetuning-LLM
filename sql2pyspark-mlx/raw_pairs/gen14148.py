from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.status") == "PENDING")
    .select(F.col("s.id"), F.col("s.carrier"), F.col("o.discount"))
)
