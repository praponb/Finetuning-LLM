from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").between(39000, 52000))
    .select("account_type")
    .unionByName(df_accounts.filter(F.col("customer_id") == 33).select("account_type"))
)
