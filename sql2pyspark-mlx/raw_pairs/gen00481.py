from pyspark.sql import functions as F

rollup_totals = (
    df_payments
    .alias("a")
    .join(df_orders.alias("dim"), F.col("a.order_id") == F.col("dim.id"), "left")
    .groupBy(
        F.col("dim.status").alias("order_status"),
        F.col("a.status").alias("payment_status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.sum(F.col("a.amount")).alias("total_amount"),
    )
)

result = (
    rollup_totals
    .select(
        "order_status",
        "payment_status",
        F.when(F.col("payment_status").rlike("^(SUCCESS|FAILED)$"), "CORE").when(F.col("payment_status").rlike("^(REFUNDED)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("payment_status_zone"),
    )
)
