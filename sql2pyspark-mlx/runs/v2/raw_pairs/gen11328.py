from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.filter(F.col("store_id").isNotNull()).alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "left_semi")
    .filter(F.col("payments.order_id").isNull())
    .select(F.col("payments.id"), F.col("payments.amount"), F.col("payments.paid_at"))
)
