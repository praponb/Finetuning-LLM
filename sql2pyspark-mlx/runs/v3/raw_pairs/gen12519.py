from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method")
    .agg(F.count("*").alias("num_payments"))
    .filter(F.col("num_payments") >= 9)
    .select("method", "num_payments")
)
