from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.filter(F.year("order_date") == 2025).alias("o"), F.col("p.order_id") == F.col("o.id"), "left_semi")
    .filter(F.to_date(F.col("p.paid_at")) >= "2025-04-01")
    .select(F.col("p.id"), F.col("p.amount"), F.col("p.order_id"), F.col("p.paid_at"))
    .orderBy(F.col("amount").desc(), "order_id")
)
