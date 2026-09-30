from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["FROZEN", "CLOSED"]))
    .groupBy("account_type")
    .pivot("status", ["FROZEN", "OPEN"])
    .agg(F.sum("balance"))
    .orderBy("account_type")
)
