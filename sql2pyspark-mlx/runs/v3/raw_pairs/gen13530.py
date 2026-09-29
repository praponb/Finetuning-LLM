from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.amount").between(1390, 1410) & (F.col("s.carrier") == "FedEx"))
    .select(F.col("s.order_id"), F.col("s.status"), F.col("o.amount"))
)
