from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "outer")
    .filter(~F.col("o.status").isin(["SHIPPED", "RETURNED"]))
    .select(
        F.col("p.id"),
        F.col("p.amount"),
        F.col("p.status"),
        F.col("p.order_id"),
        F.col("o.store_id"),
        F.col("o.discount"),
    )
)
