from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("o.id") == F.col("p.order_id"), "left")
    .filter(F.col("p.amount") > 960)
    .select(
        F.col("o.discount"),
        F.col("o.amount"),
        F.col("p.status"),
        F.col("p.amount").alias("payment_amount"),
    )
)
