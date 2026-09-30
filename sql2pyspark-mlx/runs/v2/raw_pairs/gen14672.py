from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status") != "CLOSED")
    .withColumn("account_type_title", F.initcap(F.trim("account_type")))
)
