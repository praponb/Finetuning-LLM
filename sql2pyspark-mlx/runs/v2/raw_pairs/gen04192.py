from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("balance") >= 65000) & (F.col("opened_date") < "2023-10-01"))
    .select("id", "opened_date", "account_type", "balance")
)
