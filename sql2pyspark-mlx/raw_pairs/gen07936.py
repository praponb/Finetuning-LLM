from pyspark.sql import functions as F

result = (
    df_payments
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .rollup(F.col("o.status").alias("order_status"), F.col("s.method").alias("method"))
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.sum(F.col("s.amount")), 2).alias("total_amount"),
        F.grouping_id().alias("grouping_level"),
    )
)
