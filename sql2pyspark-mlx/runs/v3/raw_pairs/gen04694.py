from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("o.id") == F.col("p.order_id"), "left_semi")
    .filter(F.col("o.shipping_fee") > 38)
    .select(F.col("o.id"), F.col("o.amount"), F.col("o.customer_id"))
)
