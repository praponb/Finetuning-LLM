from pyspark.sql import functions as F

result = (
    df_payments
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .groupBy(
        F.col("o.status").alias("order_status"),
        F.col("o.channel").alias("channel"),
        F.col("src.method").alias("method"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("src.amount")).alias("min_amount"),
    )
    .orderBy(F.col("min_amount").desc_nulls_last())
)
