from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.store_id").isin([24, 31]) | (F.col("o.discount") < 0.2))
    .select(
        F.col("p.paid_at"),
        F.col("p.method"),
        F.col("o.discount"),
        F.when(F.col("p.status") == "FAILED", 1).otherwise(0).alias("is_failed"),
    )
)
