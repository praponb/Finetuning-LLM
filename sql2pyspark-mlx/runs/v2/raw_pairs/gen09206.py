from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~((F.year("opened_date") >= 2025) & (F.col("balance") <= 35000) & F.col("account_type").isin(["SAVINGS", "CHECKING", "CREDIT"])))
    .select("id", "status", F.trim("account_type").alias("clean_account_type"))
)
