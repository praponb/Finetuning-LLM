from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter((F.col("p.order_id") == 25) & (F.col("o.amount") < 1160))
    .select(
        F.col("p.id"),
        F.col("p.method"),
        F.col("p.status"),
        F.col("p.amount"),
        F.col("o.status").alias("order_status"),
        F.col("o.order_date"),
    )
    .orderBy(F.col("method").desc())
)
