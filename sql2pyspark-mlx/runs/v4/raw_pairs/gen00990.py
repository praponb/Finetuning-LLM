from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("o.id") == F.col("s.order_id"), "left")
    .filter(F.col("s.cost") < 68)
    .select(
        F.col("o.amount"),
        F.col("o.customer_id"),
        F.col("o.order_date"),
        F.col("s.cost"),
    )
)
