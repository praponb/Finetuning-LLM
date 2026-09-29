from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter(F.col("p.method") == "promptpay")
    .select(
        F.col("p.id"),
        F.col("p.method"),
        F.col("p.status"),
        F.col("o.store_id"),
        F.col("p.amount").cast("int").alias("amount_int"),
    )
)
