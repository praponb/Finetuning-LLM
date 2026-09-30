from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.status") == "PENDING")
    .select(
        F.col("s.status"),
        F.col("s.carrier"),
        F.col("s.cost"),
        F.col("o.discount"),
        F.col("o.order_date"),
        F.col("o.shipping_fee"),
    )
    .orderBy("order_date")
)
