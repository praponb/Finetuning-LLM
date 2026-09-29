from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method")
    .agg(
        F.count(F.when(F.col("status") == "FAILED", 1)).alias("failed_count"),
        F.count("amount").alias("amount_count"),
    )
    .filter(F.col("failed_count") > 5)
    .orderBy(F.col("failed_count").desc(), "method")
    .limit(50)
)
