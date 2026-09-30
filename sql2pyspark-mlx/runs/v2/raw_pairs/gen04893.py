from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "right")
    .select(
        F.col("p.id"),
        F.col("p.amount"),
        F.col("p.status"),
        F.col("o.order_date"),
        F.col("p.amount").cast("int").alias("amount_int"),
    )
    .orderBy("order_date")
)
