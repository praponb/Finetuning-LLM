from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .groupBy(F.col("o.status"), F.col("o.channel"))
    .agg(
        F.round(F.avg(F.col("p.amount")), 2).alias("avg_amount"),
        F.count(F.col("p.amount")).alias("amount_count"),
    )
    .orderBy(F.col("status").desc())
)
