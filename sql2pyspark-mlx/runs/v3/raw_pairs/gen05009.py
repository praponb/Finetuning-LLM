from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.shipping_fee").between(16, 36) & F.col("s.order_id").isNull())
    .select(
        F.col("s.id"),
        F.col("s.delivered_date"),
        F.col("o.channel"),
        F.col("o.customer_id"),
        F.datediff(F.current_date(), F.col("s.shipped_date")).alias("days_since_shipped_date"),
    )
)
