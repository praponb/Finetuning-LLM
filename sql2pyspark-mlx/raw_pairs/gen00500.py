from pyspark.sql import functions as F

result = (
    df_payments
    .alias("e")
    .join(df_orders.alias("dim"), F.col("e.order_id") == F.col("dim.id"), "left")
    .groupingSets(
        [["order_status", "channel"], ["order_status"], []],
        F.col("dim.status").alias("order_status"),
        F.col("dim.channel").alias("channel"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.sum(F.col("e.amount")).alias("total_amount"),
        F.grouping_id().alias("grouping_level"),
    )
)
