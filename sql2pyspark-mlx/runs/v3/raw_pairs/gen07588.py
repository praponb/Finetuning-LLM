from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter(F.col("s.carrier") != "UPS")
    .select(
        F.col("s.delivered_date"),
        F.col("o.amount"),
        F.col("o.status"),
        F.col("o.discount"),
        F.lower(F.col("o.status")).alias("status_lower"),
    )
)
