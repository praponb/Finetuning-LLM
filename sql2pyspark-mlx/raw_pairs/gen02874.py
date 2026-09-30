from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([31, 36]))
    .groupBy(F.to_date("paid_at").alias("paid_day"))
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
    .filter(F.col("avg_amount") >= 62)
    .select("paid_day", "avg_amount")
    .orderBy("paid_day")
)
