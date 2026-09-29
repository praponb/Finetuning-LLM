from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_payments.alias("payments"), F.col("orders.id") == F.col("payments.order_id"), "left")
    .filter(~F.col("payments.status").isin(["REFUNDED", "SUCCESS"]))
    .groupBy(F.col("orders.id"))
    .agg(
        F.count(F.col("payments.id")).alias("payment_count"),
        F.avg(F.col("payments.amount")).alias("avg_amount"),
    )
    .orderBy("id")
)
