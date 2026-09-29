from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .pivot("account_type", ["SAVINGS", "LOAN", "CREDIT", "CHECKING"])
    .agg(F.count("balance"))
)
