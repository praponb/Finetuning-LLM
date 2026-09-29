from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status") == "FROZEN")
    .groupBy("customer_id")
    .pivot("account_type", ["CREDIT", "CHECKING", "LOAN", "SAVINGS"])
    .agg(F.sum("balance"))
)
