from pyspark.sql import functions as F

dim_totals = (
    df_payments
    .alias("a")
    .join(df_orders.alias("dim"), F.col("a.order_id") == F.col("dim.id"), "inner")
    .rollup(
        F.col("dim.status").alias("order_status"),
        F.col("a.status").alias("payment_status"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg(F.col("a.amount")), 2).alias("avg_amount"),
        F.grouping_id().alias("level_id"),
    )
)

result = (
    dim_totals
    .select("order_status", "payment_status", "cnt", "avg_amount", "level_id")
    .orderBy(F.col("cnt").desc_nulls_last())
)
