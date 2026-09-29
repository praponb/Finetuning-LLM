from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.month("opened_date").isin([1, 8, 9]))
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"), "account_type")
    .agg(
        F.count(F.when(F.col("account_type") == "CREDIT", 1)).alias("credit_count"),
        F.count("balance").alias("balance_count"),
    )
)
