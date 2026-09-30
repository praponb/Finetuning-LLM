from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("o.id") == F.col("p.order_id"), "left")
    .groupBy(F.col("o.id"), F.col("o.status"))
    .agg(
        F.count(F.col("p.id")).alias("payment_count"),
        F.min(F.col("p.amount")).alias("min_amount"),
        (F.max(F.col("p.amount")) - F.min(F.col("p.amount"))).alias("amount_range"),
    )
    .orderBy(F.col("min_amount").desc(), "id", "status")
    .limit(5)
)
