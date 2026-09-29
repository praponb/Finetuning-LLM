from pyspark.sql import functions as F

result = (
    df_accounts
    .withColumn("opened_date_plus_months", F.add_months("opened_date", 6))
    .withColumn("opened_date_plus_months_2", F.add_months("opened_date", 6))
)
