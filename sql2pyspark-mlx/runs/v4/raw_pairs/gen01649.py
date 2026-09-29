from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.filter(F.month("order_date").isin([1, 4, 9])).alias("o"), F.col("p.order_id") == F.col("o.id"), "left_semi")
    .filter(F.col("p.status").isin(["REFUNDED", "FAILED"]))
    .select(F.col("p.id"), F.col("p.method"), F.col("p.paid_at"))
    .orderBy("paid_at", F.col("id").desc())
)
