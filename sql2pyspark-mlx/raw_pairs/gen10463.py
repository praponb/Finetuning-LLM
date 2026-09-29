from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "inner")
    .filter(F.col("orders.shipping_fee") < 25)
    .groupBy(F.col("orders.status"))
    .agg(
        F.sum(F.when(F.col("payments.status") == "REFUNDED", F.col("payments.amount")).otherwise(0)).alias("refunded_amount"),
    )
    .orderBy("refunded_amount", "status")
    .limit(3)
)
