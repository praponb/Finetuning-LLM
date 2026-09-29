from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id") == 18)
    .groupBy("account_type")
    .pivot("status", ["OPEN", "FROZEN", "CLOSED"])
    .agg(F.sum("balance"))
)
