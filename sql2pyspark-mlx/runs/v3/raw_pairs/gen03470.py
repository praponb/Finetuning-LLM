from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "left")
    .filter(F.col("o.customer_id").isin([11, 34]) & ~F.col("p.status").isin(["FAILED", "SUCCESS", "REFUNDED"]))
    .select(
        F.col("p.amount"),
        F.col("p.status"),
        F.col("o.customer_id"),
        F.col("o.discount"),
    )
    .orderBy(F.col("customer_id").desc(), "status")
)
