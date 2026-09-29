from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter((F.col("s.carrier") == "Kerry") & (F.col("o.discount") > 0.28))
    .select(
        F.col("s.id"),
        F.col("s.delivered_date"),
        F.col("s.carrier"),
        F.col("s.order_id"),
        F.col("o.order_date"),
        F.col("o.shipping_fee"),
    )
)
