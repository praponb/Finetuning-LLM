from pyspark.sql import functions as F

filtered_payments = (
    df_payments
    .filter(F.col("status") != "FAILED")
    .withColumn("method_lower", F.lower("method"))
)

payments_stats = (
    filtered_payments
    .groupBy("method")
    .agg(F.countDistinct("amount").alias("unique_statuss"))
)

result = (
    payments_stats
    .select("method", "unique_statuss")
    .orderBy(F.col("unique_statuss").desc())
)
