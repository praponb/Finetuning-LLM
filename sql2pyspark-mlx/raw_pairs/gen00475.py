from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type") != "CHECKING")
    .select("account_type")
    .distinct()
)
