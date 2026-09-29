from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date") > "2023-05-01")
    .groupBy("status")
    .pivot("account_type", ["CREDIT", "SAVINGS"])
    .agg(F.avg("balance"))
)
