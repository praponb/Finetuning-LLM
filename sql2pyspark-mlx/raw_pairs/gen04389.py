from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .pivot("account_type", ["CREDIT", "LOAN", "SAVINGS", "CHECKING"])
    .agg(F.avg("balance"))
)
