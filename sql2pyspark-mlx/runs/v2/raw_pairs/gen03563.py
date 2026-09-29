from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type", F.month("opened_date").alias("opened_month"), "status")
    .agg(F.min("opened_date").alias("min_opened_date"))
    .orderBy("min_opened_date", "account_type")
)
