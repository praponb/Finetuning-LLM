from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("balance") <= 17000) & ((F.col("opened_date") <= "2025-09-23") | (F.col("balance") >= 29000)))
    .withColumn("opened_date_month_end", F.last_day("opened_date"))
    .orderBy("account_type", "opened_date", "id")
    .limit(50)
)
