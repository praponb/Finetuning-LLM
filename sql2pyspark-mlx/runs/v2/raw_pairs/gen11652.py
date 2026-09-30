from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status", F.year("opened_date").alias("opened_year"))
    .agg(
        F.sum(F.when(F.col("status") == "CLOSED", 1).otherwise(0)).alias("closed_count"),
    )
    .orderBy("status")
)
