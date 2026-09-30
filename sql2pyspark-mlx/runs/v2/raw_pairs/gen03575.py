from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status") == "CLOSED")
    .groupBy("account_type")
    .pivot("status", ["CLOSED", "OPEN", "FROZEN"])
    .agg(F.count("balance"))
    .orderBy("account_type")
)
