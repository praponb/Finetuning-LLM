from pyspark.sql import functions as F

result = (
    df_payments
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "inner")
    .groupBy(
        F.col("o.status").alias("order_status"),
        F.col("a.status").alias("payment_status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("a.amount")).alias("max_amount"),
    )
    .orderBy(F.col("order_status").asc_nulls_last())
)
