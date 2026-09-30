from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("o.id") == F.col("p.order_id"), "left")
    .filter(F.col("o.store_id").isNotNull())
    .groupBy(F.col("o.status"))
    .agg(
        F.count(F.col("p.id")).alias("payment_count"),
        F.max(F.col("p.amount")).alias("max_amount"),
        F.count(F.when(F.col("p.method") == "cash", 1)).alias("cash_count"),
    )
)
