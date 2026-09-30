from pyspark.sql import functions as F

base = (
    df_payments
    .filter(F.col("status") == "SUCCESS")
    .withColumn("paid_at_day", F.col("paid_at").cast("date"))
    .withColumn("method_filled", F.coalesce("method", F.lit("N/A")))
)

result = (
    base
    .alias("src")
    .join(df_orders.alias("dim"), F.col("src.order_id") == F.col("dim.id"), "inner")
    .groupingSets(
        [["channel", "payment_status"], ["channel"], []],
        F.col("dim.channel").alias("channel"),
        F.col("src.status").alias("payment_status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("src.amount")).alias("max_amount"),
    )
    .orderBy(F.col("channel").desc_nulls_last())
)
