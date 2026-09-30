from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.filter(F.col("shipping_fee") >= 34).alias("o"), F.col("p.order_id") == F.col("o.id"), "left_semi")
    .filter(F.to_date(F.col("p.paid_at")) < "2024-11-14")
    .select(F.col("p.id"), F.col("p.method"), F.col("p.order_id"))
    .orderBy(F.col("order_id").desc())
)
