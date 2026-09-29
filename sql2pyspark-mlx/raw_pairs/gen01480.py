from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter((F.col("o.order_date") <= "2024-06-01") & (F.col("p.paid_at") >= "2025-10-01"))
    .select(
        F.col("p.id"),
        F.col("p.status"),
        F.col("p.order_id"),
        F.col("p.method"),
        F.col("o.discount"),
        F.col("o.store_id"),
        F.date_trunc("MONTH", F.col("p.paid_at")).alias("paid_at_period"),
    )
    .orderBy(F.col("paid_at_period").desc())
)
