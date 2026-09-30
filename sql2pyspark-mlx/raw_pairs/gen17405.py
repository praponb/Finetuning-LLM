from pyspark.sql import functions as F

valid_accounts = df_accounts.filter(F.col("balance").between(52000, 71000))

opened_month_summary = (
    valid_accounts
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(F.count(F.when(F.col("account_type") == "CREDIT", 1)).alias("credit_count"))
)

result = (
    opened_month_summary
    .filter(F.col("credit_count") < 8)
    .select("opened_month", "credit_count")
)
