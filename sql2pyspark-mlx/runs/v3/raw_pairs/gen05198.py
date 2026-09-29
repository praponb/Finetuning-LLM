from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") >= 72000)
    .select("status")
    .unionByName(df_accounts.filter(F.col("account_type") != "LOAN").select("status"))
    .distinct()
)
