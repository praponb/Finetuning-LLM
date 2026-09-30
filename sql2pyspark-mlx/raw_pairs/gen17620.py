from pyspark.sql import functions as F

dim_totals = (
    df_payments
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "inner")
    .cube(
        F.col("d.channel").alias("channel"),
        F.col("a.status").alias("payment_status"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("a.amount")).alias("min_amount"),
    )
)

result = (
    dim_totals
    .select("channel", "payment_status", "min_amount")
    .orderBy("payment_status")
)
