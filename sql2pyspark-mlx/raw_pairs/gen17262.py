from pyspark.sql import functions as F

filtered = (
    df_payments
    .filter((F.col("status") != "SUCCESS") & (F.col("paid_at") > F.lit("2023-10-14 18:00:00").cast("timestamp")))
    .select(
        "id",
        "order_id",
        "amount",
        "status",
        F.concat_ws("-", "status", "method").alias("status_method_key"),
    )
)

result = (
    filtered
    .alias("s")
    .join(df_orders.alias("dim"), F.col("s.order_id") == F.col("dim.id"), "inner")
    .cube(
        F.col("dim.channel").alias("channel"),
        F.col("s.status").alias("payment_status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.sum(F.col("s.amount")).alias("total_amount"),
        F.grouping_id().alias("grouping_level"),
    )
)
