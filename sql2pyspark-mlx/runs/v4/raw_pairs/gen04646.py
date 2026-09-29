from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").between(50000, 69000))
    .select("id", "opened_date", "account_type", "status")
)
