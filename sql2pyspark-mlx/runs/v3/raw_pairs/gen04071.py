from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["REFUNDED", "SUCCESS"]))
    .groupBy(F.year("paid_at").alias("paid_year"))
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
    .orderBy("avg_amount", "paid_year")
    .limit(10)
)
