from pyspark.sql import functions as F

result = (
    df_accounts
    .agg(
        F.count(F.when(F.col("account_type") == "LOAN", 1)).alias("loan_count"),
        F.sum(F.when(F.col("status") == "CLOSED", 1).otherwise(0)).alias("closed_count"),
        F.min("opened_date").alias("min_opened_date"),
    )
)
