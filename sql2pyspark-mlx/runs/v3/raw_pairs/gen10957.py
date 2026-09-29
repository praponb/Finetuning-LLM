from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter(F.col("p.order_id").isin([10, 16]))
    .select(
        F.col("o.store_id"),
        F.col("o.order_date"),
        F.col("o.discount"),
        F.col("p.amount"),
        F.col("p.status"),
        F.col("p.order_id"),
    )
)
