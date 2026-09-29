from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([11, 35]))
    .withColumn("abs_balance", F.abs("balance"))
    .withColumn("opened_date_period", F.date_trunc("week", "opened_date"))
    .orderBy(F.col("balance").desc(), "id")
    .limit(50)
)
