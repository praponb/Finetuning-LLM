from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").between(25000, 35000))
    .select("account_type")
    .distinct()
)
