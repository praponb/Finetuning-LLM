from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter(F.col("p.paid_at") < "2025-10-04")
    .select(
        F.col("o.order_date"),
        F.col("p.status"),
        F.col("p.method"),
        F.col("p.order_id"),
    )
    .orderBy("order_date", F.col("status").desc())
)
