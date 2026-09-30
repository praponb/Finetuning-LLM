from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") < 20000)
    .groupBy("account_type")
    .pivot("status", ["OPEN", "CLOSED", "FROZEN"])
    .agg(F.avg("balance"))
)
