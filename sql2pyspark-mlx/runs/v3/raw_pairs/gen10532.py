from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("o.id") == F.col("p.order_id"), "left")
    .filter(F.month(F.col("o.order_date")).isin([3, 6, 7]) & (F.col("o.status") == "CANCELLED"))
    .select(
        F.col("o.discount"),
        F.col("o.store_id"),
        F.col("p.method"),
        F.col("p.status"),
    )
    .orderBy("method")
)
