from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "right")
    .filter(F.month(F.col("o.order_date")).isin([1, 2, 8]) & (F.col("o.shipping_fee") < 40))
    .select(
        F.col("s.cost"),
        F.col("s.order_id"),
        F.col("o.customer_id"),
        F.col("o.store_id"),
    )
)
