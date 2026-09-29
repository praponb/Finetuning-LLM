from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "outer")
    .filter(F.col("o.discount") <= 0.38)
    .select(
        F.col("s.id"),
        F.col("s.carrier"),
        F.col("s.delivered_date"),
        F.col("s.order_id"),
        F.col("o.channel"),
        F.col("o.shipping_fee"),
        F.add_months(F.col("s.delivered_date"), 6).alias("delivered_date_plus_months"),
    )
    .orderBy("delivered_date_plus_months", "order_id")
)
