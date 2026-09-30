from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("o.id") == F.col("p.order_id"), "left")
    .select(F.col("o.order_date"), F.col("p.paid_at"))
    .orderBy(F.col("order_date").desc())
)
