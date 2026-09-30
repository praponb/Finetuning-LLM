from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.discount") <= 0.19)
    .select(
        F.col("s.id"),
        F.col("s.delivered_date"),
        F.col("s.carrier"),
        F.col("o.status"),
        F.col("o.customer_id"),
    )
    .orderBy(F.col("carrier").desc())
)
