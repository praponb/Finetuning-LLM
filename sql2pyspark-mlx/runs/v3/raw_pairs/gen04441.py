from pyspark.sql import functions as F

result = (
    df_accounts
    .select("status", "account_type")
    .distinct()
    .orderBy("status", "account_type")
    .limit(10)
)
