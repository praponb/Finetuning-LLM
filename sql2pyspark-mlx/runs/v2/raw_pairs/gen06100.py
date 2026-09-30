from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("s.order_id") == F.col("o.id"), "left")
    .select(
        F.col("o.order_date"),
        F.col("o.discount"),
        F.col("s.cost"),
        F.col("s.status"),
    )
)
