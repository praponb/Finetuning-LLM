from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter(F.to_date(F.col("p.paid_at")) < "2023-11-24")
    .groupBy(F.col("o.channel"), F.col("o.status"))
    .agg(F.sum(F.col("p.amount")).alias("total_amount"))
    .orderBy("channel")
)
