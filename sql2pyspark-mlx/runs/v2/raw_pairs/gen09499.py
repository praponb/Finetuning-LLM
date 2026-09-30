from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id") == 37)
    .select("id", "balance", "opened_date", "account_type")
    .orderBy("account_type", F.col("id").desc())
)
