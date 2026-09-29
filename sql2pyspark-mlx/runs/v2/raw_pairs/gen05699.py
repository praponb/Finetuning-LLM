from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "right")
    .groupBy(F.col("o.customer_id"))
    .agg(
        F.count(F.when(F.col("p.method") == "paypal", 1)).alias("paypal_count"),
        F.max(F.col("p.amount")).alias("max_amount"),
    )
    .orderBy("paypal_count")
)
