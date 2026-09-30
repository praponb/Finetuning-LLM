from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status") != "SUCCESS")
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0)).alias("success_count"),
    )
    .orderBy(F.col("success_count").desc())
)
