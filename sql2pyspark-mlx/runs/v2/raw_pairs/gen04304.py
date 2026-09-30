from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.filter(F.col("customer_id").isin([14, 19, 26, 36])).alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "left_semi")
    .filter(F.to_date(F.col("payments.paid_at")) >= "2023-07-01")
    .select(F.col("payments.id"), F.col("payments.amount"))
)
