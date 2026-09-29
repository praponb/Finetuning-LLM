from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").isNotNull())
    .select("opened_date")
    .unionByName(df_accounts.filter(F.col("status").isNull()).select("opened_date"))
)
