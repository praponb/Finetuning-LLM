from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .select(
        F.col("p.amount"),
        F.col("o.order_date"),
        F.col("s.opened_date"),
        F.col("s.city"),
    )
    .orderBy("order_date")
)
