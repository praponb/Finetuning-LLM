from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "outer")
    .filter(F.col("p.status").isin(["SUCCESS", "FAILED", "REFUNDED"]) & (F.col("p.amount") > 510))
    .select(
        F.col("p.amount"),
        F.col("p.method"),
        F.col("o.order_date"),
        F.col("o.customer_id"),
    )
)
