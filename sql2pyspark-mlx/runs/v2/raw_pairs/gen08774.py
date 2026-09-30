from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "outer")
    .filter(F.col("s.order_id").isin([13, 37]))
    .select(
        F.col("s.id"),
        F.col("s.delivered_date"),
        F.col("s.cost"),
        F.col("o.order_date"),
        F.col("o.shipping_fee"),
        (F.col("o.amount") * F.col("o.discount")).alias("amount_times_discount"),
    )
)
