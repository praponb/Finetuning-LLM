from pyspark.sql import functions as F

result = (
    df_payments
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["order_status", "method"], ["order_status"]],
        F.col("p.status").alias("order_status"),
        F.col("e.method").alias("method"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.sum(F.col("e.amount")).alias("total_amount"),
    )
)
