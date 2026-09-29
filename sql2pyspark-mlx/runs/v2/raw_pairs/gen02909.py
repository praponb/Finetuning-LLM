from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type") != "SAVINGS")
    .groupBy(F.month("opened_date").alias("opened_month"), "account_type")
    .agg(F.count(F.when(F.col("status") == "OPEN", 1)).alias("open_count"))
    .orderBy(F.col("account_type").desc(), "opened_month")
    .limit(5)
)
