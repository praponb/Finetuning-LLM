from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "outer")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("o.store_id").isin([25, 36]))
    .select(
        F.col("p.id"),
        F.col("p.method"),
        F.col("p.order_id"),
        F.col("o.customer_id"),
        F.col("o.discount"),
        F.col("s.region"),
    )
    .orderBy("region", F.col("id").desc())
)
