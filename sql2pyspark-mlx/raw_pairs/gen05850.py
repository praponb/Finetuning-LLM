from pyspark.sql import functions as F

result = (
    df_payments
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "inner")
    .groupingSets(
        [["order_status", "method"], ["order_status"], []],
        F.col("o.status").alias("order_status"),
        F.col("a.method").alias("method"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.avg(F.col("a.amount")).alias("avg_amount"),
    )
    .orderBy(F.col("method").desc_nulls_last())
)
