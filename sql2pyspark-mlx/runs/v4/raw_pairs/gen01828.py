from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "left")
    .filter((F.col("s.carrier") == "DHL") & (F.col("s.carrier") == "UPS"))
    .select(
        F.col("s.id"),
        F.col("s.delivered_date"),
        F.col("o.customer_id"),
        F.col("o.channel"),
    )
    .orderBy(F.col("id").desc(), "delivered_date")
    .limit(50)
)
