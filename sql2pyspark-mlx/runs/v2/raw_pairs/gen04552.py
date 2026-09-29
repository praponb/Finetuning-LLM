from pyspark.sql import functions as F

src = (
    df_payments
    .filter(F.col("method") != "bank_transfer")
    .withColumn("paid_at_period", F.date_trunc("WEEK", "paid_at"))
)

agg = (
    src
    .groupBy("method")
    .agg(F.sum("amount").alias("total_amount"))
)

result = agg.orderBy("method", "total_amount")
