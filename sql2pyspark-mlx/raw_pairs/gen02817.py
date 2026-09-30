from pyspark.sql import functions as F

result = (
    df_orders
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "inner")
    .rollup(F.col("d.segment").alias("segment"), F.col("src.status").alias("status"))
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("src.shipping_fee")).alias("max_shipping_fee"),
        F.round(F.sum(F.col("src.amount")), 2).alias("total_amount"),
    )
    .orderBy(F.col("total_amount").desc())
)
