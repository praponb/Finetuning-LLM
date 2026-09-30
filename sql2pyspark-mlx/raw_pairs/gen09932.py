from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("o.id") == F.col("s.order_id"), "left")
    .filter(F.col("o.customer_id").isin([19, 27]) & (F.col("o.shipping_fee") >= 26))
    .select(
        F.col("o.shipping_fee"),
        F.col("s.cost"),
        F.col("s.shipped_date"),
        F.col("s.carrier"),
    )
)
