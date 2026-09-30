from pyspark.sql import functions as F

result = (
    df_payments
    .alias("e")
    .join(df_orders.alias("dim"), F.col("e.order_id") == F.col("dim.id"), "inner")
    .groupingSets(
        [["order_status", "method"], ["order_status"]],
        F.col("dim.status").alias("order_status"),
        F.col("e.method").alias("method"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.avg(F.col("e.amount")), 2).alias("avg_amount"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy(F.col("order_status").desc_nulls_first())
)
