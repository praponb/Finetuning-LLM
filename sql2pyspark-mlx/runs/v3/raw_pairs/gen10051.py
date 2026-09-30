from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .filter(F.col("o.status").isin(["CANCELLED", "RETURNED"]) & (F.col("s.shipped_date") <= "2025-04-01"))
    .select(
        F.col("s.carrier"),
        F.col("s.cost"),
        F.col("s.delivered_date"),
        F.col("o.amount"),
        F.col("o.status"),
        F.col("o.shipping_fee"),
    )
)
