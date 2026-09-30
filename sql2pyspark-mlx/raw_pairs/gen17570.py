from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type") == "SAVINGS")
    .groupBy("status")
    .pivot("account_type", ["CHECKING", "CREDIT"])
    .agg(F.max("balance"))
    .orderBy("status")
)
