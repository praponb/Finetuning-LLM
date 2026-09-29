from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status") == "CLOSED")
    .withColumn("balance_filled", F.coalesce("balance", F.lit(0)))
    .withColumn("opened_date_period", F.date_trunc("month", "opened_date"))
    .orderBy("account_type")
)
