from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").between(16000, 22000))
    .select("status", "account_type")
    .distinct()
)
