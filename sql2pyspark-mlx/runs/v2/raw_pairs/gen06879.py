from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type") == "CREDIT")
    .withColumn("opened_date_minus_80d", F.date_sub("opened_date", 80))
    .withColumn("opened_date_year", F.year("opened_date"))
    .orderBy("account_type", F.col("balance").desc(), "id")
    .limit(3)
)
