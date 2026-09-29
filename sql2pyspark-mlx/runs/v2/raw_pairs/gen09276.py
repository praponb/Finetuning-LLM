from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type", F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(
        F.sum(F.when(F.col("account_type") == "CREDIT", F.col("balance")).otherwise(0)).alias("credit_balance"),
    )
    .orderBy("opened_month", "account_type")
    .limit(50)
)
