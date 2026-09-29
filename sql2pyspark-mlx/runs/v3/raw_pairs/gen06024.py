from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter(F.col("p.order_id") == 1)
    .select(
        F.col("o.store_id"),
        F.col("o.status"),
        F.col("p.method"),
        F.col("p.amount"),
    )
    .orderBy("method")
)
