from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("o.id") == F.col("s.order_id"), "left")
    .filter(F.col("o.channel") == "mobile")
    .select(
        F.col("o.customer_id"),
        F.col("o.order_date"),
        F.col("o.discount"),
        F.col("s.shipped_date"),
        F.col("s.carrier"),
        F.col("s.delivered_date"),
    )
)
