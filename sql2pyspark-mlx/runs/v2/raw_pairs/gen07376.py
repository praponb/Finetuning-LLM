from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.status").isin(["CANCELLED", "RETURNED"]) & (F.col("o.customer_id") == 1))
    .select(F.col("s.id"), F.col("s.delivered_date"), F.col("o.discount"))
    .orderBy("id", "delivered_date")
)
