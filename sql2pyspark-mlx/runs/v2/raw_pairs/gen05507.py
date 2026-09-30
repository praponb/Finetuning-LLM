from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "left")
    .filter((F.col("o.order_date") < "2023-07-01") & F.col("s.order_id").isNull())
    .select(
        F.col("s.id"),
        F.col("s.delivered_date"),
        F.col("o.customer_id"),
        F.col("o.discount"),
        F.lower(F.col("s.status")).alias("status_lower"),
    )
)
