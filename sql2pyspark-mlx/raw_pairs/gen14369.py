from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") <= 690)
    .groupBy("method", "status")
    .agg(F.count("*").alias("cnt"))
    .filter(F.col("cnt") < 3)
    .select("method", "status", "cnt")
    .orderBy("cnt")
)
