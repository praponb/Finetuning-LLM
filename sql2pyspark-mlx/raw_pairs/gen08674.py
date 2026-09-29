from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .pivot("account_type", ["SAVINGS", "CHECKING", "CREDIT", "LOAN"])
    .agg(F.sum("balance"))
)
