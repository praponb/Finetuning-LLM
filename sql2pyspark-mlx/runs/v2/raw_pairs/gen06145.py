from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .groupBy(F.date_format(F.col("o.order_date"), "yyyy-MM").alias("order_month"))
    .agg(
        F.round(F.avg(F.col("p.amount")), 2).alias("avg_amount"),
        F.max(F.col("p.amount")).alias("max_amount"),
    )
    .filter(F.col("avg_amount") >= 75)
)
