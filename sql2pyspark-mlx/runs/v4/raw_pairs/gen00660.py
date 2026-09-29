from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("customer_id", "account_type")
    .pivot("status", ["CLOSED", "OPEN"])
    .agg(F.max("balance").alias("total"), F.count(F.lit(1)).alias("cnt"))
)
