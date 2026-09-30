from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "right")
    .filter(F.col("o.amount") >= 1170)
    .select(
        F.col("p.id"),
        F.col("p.order_id"),
        F.col("o.order_date"),
        F.col("o.channel"),
    )
    .orderBy("order_date", F.col("channel").desc())
)
