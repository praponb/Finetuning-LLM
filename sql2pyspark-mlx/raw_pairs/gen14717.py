from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") < 54000)
    .agg(
        F.count(F.when(F.col("status") == "CLOSED", 1)).alias("closed_count"),
        F.min("opened_date").alias("first_opened_date"),
    )
)
