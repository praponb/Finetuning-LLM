from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.year("opened_date") == 2023)
    .select("account_type")
    .unionByName(df_accounts.filter(F.col("balance") >= 31000).select("account_type"))
    .distinct()
    .orderBy("account_type")
)
