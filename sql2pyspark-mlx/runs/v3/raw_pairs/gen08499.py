from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.year("opened_date") >= 2023)
    .select("id", "account_type", "balance", "status")
    .orderBy(F.col("account_type").desc(), "status")
)
