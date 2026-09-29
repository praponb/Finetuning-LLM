from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").isNotNull())
    .groupBy("method")
    .agg(
        F.sum(F.when(F.col("status") == "SUCCESS", F.col("amount")).otherwise(0)).alias("success_amount"),
    )
    .filter(F.col("success_amount") >= 64)
    .select("method", "success_amount")
    .orderBy("method")
)
