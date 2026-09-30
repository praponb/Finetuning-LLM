from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter(F.month(F.col("o.order_date")).isin([10, 11, 12]) | F.month(F.col("s.shipped_date")).isin([3, 5, 7]))
    .select(
        F.col("s.order_id"),
        F.col("s.delivered_date"),
        F.col("o.customer_id"),
        F.col("o.discount"),
        F.col("o.shipping_fee"),
    )
)
