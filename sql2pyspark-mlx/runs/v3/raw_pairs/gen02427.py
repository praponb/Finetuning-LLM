from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type")
    .pivot("status", ["OPEN", "FROZEN"])
    .agg(F.sum("balance"))
)
