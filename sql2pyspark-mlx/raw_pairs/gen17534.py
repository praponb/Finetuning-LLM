from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(F.col("p.status") == "FAILED")
    .groupBy(F.col("s.city"))
    .agg(
        (F.max(F.col("p.amount")) - F.min(F.col("p.amount"))).alias("amount_range"),
        F.round(F.avg(F.col("p.amount")), 2).alias("avg_amount"),
    )
)
