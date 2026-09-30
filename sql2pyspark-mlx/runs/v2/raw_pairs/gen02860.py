from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("opened_date") <= "2023-05-20") & (F.col("balance") <= 33000))
    .select("opened_date", F.trim("account_type").alias("clean_account_type"))
    .orderBy("clean_account_type")
)
