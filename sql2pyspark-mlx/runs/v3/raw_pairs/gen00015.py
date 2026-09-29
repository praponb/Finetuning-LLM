from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").between(56000, 76000))
    .select("id", "balance", "account_type")
    .orderBy(F.col("id").desc(), "balance")
)
