from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id") == 7)
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"), "account_type")
    .agg(F.round(F.avg("balance"), 2).alias("avg_balance"))
    .filter(F.col("avg_balance") < 31)
    .orderBy(F.col("account_type").desc(), F.col("avg_balance").desc())
)
