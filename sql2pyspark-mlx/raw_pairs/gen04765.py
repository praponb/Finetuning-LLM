from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .pivot("account_type", ["CHECKING", "CREDIT", "SAVINGS"])
    .agg(F.sum("balance").alias("amount"), F.count(F.lit(1)).alias("cnt"))
)
