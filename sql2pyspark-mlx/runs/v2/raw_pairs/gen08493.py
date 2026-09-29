from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "outer")
    .filter(F.col("payments.method").isin(["promptpay", "card", "bank_transfer"]))
    .select(
        F.col("payments.method"),
        F.col("payments.status"),
        F.col("orders.order_date"),
        F.col("orders.customer_id"),
        F.add_months(F.col("orders.order_date"), 12).alias("order_date_plus_months"),
    )
    .orderBy(F.col("order_date").desc(), "method")
)
