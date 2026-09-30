from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(
        F.sum(F.when(F.col("account_type") == "LOAN", F.col("balance")).otherwise(0)).alias("loan_balance"),
        F.max("opened_date").alias("latest_opened_date"),
    )
    .filter(F.col("loan_balance") < 80)
    .orderBy(F.col("opened_month").desc())
)
