from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("balance") >= 72000) & (F.col("balance") > 60000))
    .select("status", "account_type")
    .orderBy("status")
)
