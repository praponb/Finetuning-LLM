from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "right")
    .select(
        F.col("p.method"),
        F.col("p.amount"),
        F.col("o.order_date"),
        F.col("o.status"),
        F.col("s.region"),
        F.col("s.opened_date"),
        (F.col("o.amount") * F.col("o.discount")).alias("amount_times_discount"),
    )
    .orderBy("region")
)
