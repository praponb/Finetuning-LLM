from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("o.id") == F.col("p.order_id"), "left")
    .filter(F.col("o.channel").isin(["store", "mobile"]))
    .select(
        F.col("o.channel"),
        F.col("o.status"),
        F.col("p.status").alias("payment_status"),
        F.col("p.method"),
    )
    .orderBy(F.col("channel").desc())
)
