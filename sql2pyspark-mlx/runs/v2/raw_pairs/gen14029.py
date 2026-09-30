from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter(F.col("p.status").isin(["SUCCESS", "FAILED", "REFUNDED"]))
    .groupBy(F.col("o.channel"))
    .agg(F.min(F.col("p.amount")).alias("min_amount"))
    .orderBy("min_amount", "channel")
    .limit(10)
)
