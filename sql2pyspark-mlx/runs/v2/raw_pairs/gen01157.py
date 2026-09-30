from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["LOAN", "CREDIT", "CHECKING"]) & (F.col("customer_id").isNull() | (F.col("account_type") == "CHECKING")))
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(
        F.sum(F.when(F.col("account_type") == "LOAN", F.col("balance")).otherwise(0)).alias("loan_balance"),
        F.avg("balance").alias("avg_balance"),
    )
)
