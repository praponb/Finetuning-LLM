from pyspark.sql import functions as F

result = (
    df_accounts
    .select("account_type", "status")
    .distinct()
    .orderBy(F.col("status").desc(), "account_type")
    .limit(50)
)
