from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") > 41000)
    .withColumn("opened_date_period", F.date_trunc("year", "opened_date"))
    .withColumn("abs_balance", F.abs("balance"))
    .orderBy(F.col("opened_date_period").desc())
)
