from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method")
    .agg(F.max("amount").alias("max_amount"))
    .filter(F.col("max_amount") > 1000)
    .select("method", "max_amount")
    .orderBy(F.col("max_amount").desc(), "method")
    .limit(5)
)
