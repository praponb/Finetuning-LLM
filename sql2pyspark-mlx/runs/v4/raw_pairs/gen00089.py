from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .filter(F.col("s.carrier") == "FedEx")
    .select(
        F.col("s.id"),
        F.col("s.status"),
        F.col("o.customer_id"),
        F.col("o.discount"),
        F.col("o.amount"),
    )
)
