from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") > 1300)
    .groupBy(F.date_format("paid_at", "yyyy-MM").alias("paid_month"))
    .agg(F.count("*").alias("num_payments"))
    .filter(F.col("num_payments") > 4)
    .select("paid_month", "num_payments")
    .orderBy("paid_month", "num_payments")
)
