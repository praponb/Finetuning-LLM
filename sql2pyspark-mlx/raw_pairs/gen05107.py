from pyspark.sql import functions as F

result = (
    df_accounts
    .select("status", "account_type", "balance")
    .orderBy("account_type")
)
