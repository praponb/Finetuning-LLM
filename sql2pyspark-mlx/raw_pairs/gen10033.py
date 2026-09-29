from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type") == "CHECKING")
    .groupBy("status")
    .pivot("account_type", ["CREDIT", "SAVINGS", "CHECKING", "LOAN"])
    .agg(F.max("balance"))
)
