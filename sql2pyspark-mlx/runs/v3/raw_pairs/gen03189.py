from pyspark.sql import functions as F

result = (
    df_accounts
    .withColumn("opened_date_plus_months", F.add_months("opened_date", 1))
    .orderBy(F.col("balance").desc(), "id")
    .limit(50)
)
