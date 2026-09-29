from pyspark.sql import functions as F

result = (
    df_payments
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "inner")
    .rollup(F.col("d.channel").alias("channel"), F.col("src.method").alias("method"))
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("src.amount")).alias("max_amount"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("channel").asc_nulls_first())
)
