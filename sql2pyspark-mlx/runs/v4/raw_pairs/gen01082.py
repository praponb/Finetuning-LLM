from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").between(19000, 21000))
    .select("account_type")
    .unionByName(df_accounts.filter(F.col("balance") <= 22000).select("account_type"))
    .distinct()
)
