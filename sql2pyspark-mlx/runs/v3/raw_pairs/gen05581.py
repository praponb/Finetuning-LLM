from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["FROZEN", "CLOSED", "OPEN"]) | F.col("balance").between(36000, 79000))
    .select("id", "customer_id", "account_type", "balance")
    .orderBy("account_type")
)
