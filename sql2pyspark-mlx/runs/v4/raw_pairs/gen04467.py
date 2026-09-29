from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(
        F.sum("balance").alias("total_balance"),
        F.sum(F.when(F.col("account_type") == "LOAN", 1).otherwise(0)).alias("loan_count"),
    )
    .filter(F.col("total_balance") < 458000)
    .select("opened_month", "total_balance", "loan_count")
)
