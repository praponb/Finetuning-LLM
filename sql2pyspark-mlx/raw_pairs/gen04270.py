from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type") == "CHECKING")
    .select("id", "account_type")
    .orderBy(F.col("id").desc(), "account_type")
    .limit(3)
)
