from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.filter(F.col("shipping_fee") > 29).alias("o"), F.col("p.order_id") == F.col("o.id"), "left_semi")
    .select(F.col("p.id"), F.col("p.amount"), F.col("p.paid_at"))
    .orderBy("amount", "id")
    .limit(1)
)
