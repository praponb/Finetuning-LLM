from pyspark.sql import functions as F

result = (
    df_payments
    .alias("a")
    .join(df_orders.alias("dim"), F.col("a.order_id") == F.col("dim.id"), "left")
    .groupingSets(
        [["order_status", "payment_status"], ["order_status"]],
        F.col("dim.status").alias("order_status"),
        F.col("a.status").alias("payment_status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.avg(F.col("a.amount")), 2).alias("avg_amount"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy(F.col("order_status").desc_nulls_first())
)
