from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id") == 1)
    .select("id", "account_type", "balance")
    .orderBy(F.col("balance").desc(), "id")
    .limit(20)
)
