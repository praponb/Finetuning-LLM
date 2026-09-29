from pyspark.sql import functions as F

rollup_totals = (
    df_payments
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "inner")
    .groupingSets(
        [["channel", "order_status"], ["channel"]],
        F.col("o.channel").alias("channel"),
        F.col("o.status").alias("order_status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("src.amount")).alias("max_amount"),
        F.grouping_id().alias("gid"),
    )
)

result = (
    rollup_totals
    .select("channel", "gid")
    .orderBy(F.col("gid").desc())
)
