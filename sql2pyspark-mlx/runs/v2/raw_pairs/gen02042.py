from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .filter(F.col("payments.order_id").isNotNull())
    .join(df_orders.filter(F.col("shipping_fee") > 30).alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "left_anti")
    .select(F.col("payments.id"), F.col("payments.method"))
)
