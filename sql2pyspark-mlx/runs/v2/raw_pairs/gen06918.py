from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter(F.col("p.amount").between(650, 1180))
    .select(
        F.col("p.id"),
        F.col("p.paid_at"),
        F.col("o.amount"),
        F.col("o.status"),
        F.col("o.store_id"),
        F.concat(F.col("p.status"), F.lit("/"), F.col("p.method")).alias("status_method"),
    )
)
