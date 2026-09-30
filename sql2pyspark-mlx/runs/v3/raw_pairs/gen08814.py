from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(F.col("p.amount") <= 630)
    .select(
        F.col("p.id"),
        F.col("p.order_id"),
        F.col("o.discount"),
        F.col("o.amount"),
        F.col("s.opened_date"),
        F.round(F.col("p.amount"), 0).alias("amount_rounded"),
    )
    .orderBy("amount_rounded")
)
