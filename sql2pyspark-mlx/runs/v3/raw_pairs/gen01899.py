from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .filter(F.month(F.col("s.shipped_date")).isin([2, 9, 11]))
    .select(
        F.col("s.id"),
        F.col("s.status"),
        F.col("o.shipping_fee"),
        F.col("o.customer_id"),
    )
)
