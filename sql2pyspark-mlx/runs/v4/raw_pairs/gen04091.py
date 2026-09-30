from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status") == "FROZEN")
    .groupBy("status")
    .pivot("account_type", ["LOAN", "CREDIT", "SAVINGS"])
    .agg(F.sum("balance"))
)
