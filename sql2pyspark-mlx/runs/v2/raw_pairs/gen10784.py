from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .pivot("account_type", ["SAVINGS", "CREDIT", "LOAN", "CHECKING"])
    .agg(F.sum("balance"))
    .orderBy("status")
)
