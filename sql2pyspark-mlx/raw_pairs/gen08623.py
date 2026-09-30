from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").between(63000, 69000) & ((F.col("customer_id") == 25) | F.col("customer_id").isin([2, 27, 32, 33])))
    .select("opened_date", "status", "account_type")
)
