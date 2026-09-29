from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type", F.year("opened_date").alias("opened_year"))
    .agg(F.avg("balance").alias("avg_balance"))
    .filter(F.col("avg_balance") >= 54000)
    .select("account_type", "opened_year", "avg_balance")
)
