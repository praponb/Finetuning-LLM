from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "left")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("orders.customer_id"), "left")
    .filter(~F.col("payments.status").isin(["SUCCESS", "FAILED", "REFUNDED"]))
    .groupBy(
        F.month(F.col("orders.order_date")).alias("order_month"),
        F.col("orders.customer_id"),
    )
    .agg(F.max(F.col("payments.paid_at")).alias("last_paid_at"))
    .orderBy("last_paid_at")
)
