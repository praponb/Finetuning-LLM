from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "left")
    .select(
        F.col("p.status"),
        F.col("p.amount"),
        F.col("o.order_date"),
        F.length(F.col("p.status")).alias("status_length"),
    )
)
