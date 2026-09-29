from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .groupBy(F.col("o.channel"))
    .agg(F.round(F.avg(F.col("p.amount")), 2).alias("avg_amount"))
    .filter(F.col("avg_amount") >= 20)
)
