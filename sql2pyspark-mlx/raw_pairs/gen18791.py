from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["OPEN", "CLOSED", "FROZEN"]))
    .groupBy("account_type")
    .pivot("status", ["FROZEN", "CLOSED"])
    .agg(F.count("balance"))
)
