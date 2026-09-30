from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([9, 17, 27, 35]))
    .withColumn("status_account_type", F.concat("status", F.lit(" "), "account_type"))
    .orderBy("opened_date", F.col("customer_id").desc())
)
