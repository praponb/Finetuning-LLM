from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.amount").between(940, 1340))
    .select(
        F.col("o.channel"),
        F.col("o.status"),
        F.col("o.customer_id"),
        F.col("p.method"),
        F.col("p.amount"),
    )
    .orderBy(F.col("status").desc())
)
