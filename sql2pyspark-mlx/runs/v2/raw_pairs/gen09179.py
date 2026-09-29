from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.filter(F.col("shipping_fee") < 21).alias("o"), F.col("p.order_id") == F.col("o.id"), "left_semi")
    .filter(F.to_date(F.col("p.paid_at")) < "2023-04-01")
    .select(F.col("p.id"), F.col("p.order_id"))
)
