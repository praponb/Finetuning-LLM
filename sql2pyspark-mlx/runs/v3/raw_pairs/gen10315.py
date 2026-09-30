from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "left")
    .filter(F.col("o.customer_id").isNull() & (F.col("s.status") == "LOST"))
    .select(
        F.col("s.cost"),
        F.col("s.status"),
        F.col("o.order_date"),
        F.col("o.channel"),
        F.col("o.shipping_fee"),
    )
)
