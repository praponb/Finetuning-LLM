from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status") == "FROZEN")
    .select("id", "customer_id", "account_type", "balance")
    .orderBy(F.col("id").desc(), F.col("balance").desc())
    .limit(5)
)
