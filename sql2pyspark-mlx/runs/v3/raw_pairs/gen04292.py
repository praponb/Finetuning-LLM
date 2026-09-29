from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") >= 72000)
    .select("account_type")
    .distinct()
)
