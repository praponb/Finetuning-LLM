from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.filter(F.col("status").isin(["COMPLETED", "CANCELLED", "PENDING"])).alias("o"), F.col("p.order_id") == F.col("o.id"), "left_semi")
    .filter(F.col("p.paid_at") < "2024-10-01")
    .select(F.col("p.id"), F.col("p.paid_at"), F.col("p.amount"), F.col("p.status"))
)
