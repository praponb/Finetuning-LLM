from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("o.status") != "PENDING")
    .groupBy(F.col("o.customer_id"))
    .agg(
        F.count(F.col("p.amount")).alias("amount_count"),
        (F.max(F.col("p.amount")) - F.min(F.col("p.amount"))).alias("amount_range"),
    )
    .filter(F.col("amount_count") >= 4)
)
