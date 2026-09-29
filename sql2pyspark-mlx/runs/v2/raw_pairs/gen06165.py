from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").between(720, 1300) & (F.col("method") != "card"))
    .groupBy("status")
    .agg(F.max("paid_at").alias("latest_paid_at"))
)
