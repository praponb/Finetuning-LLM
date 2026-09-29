from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date") > "2024-01-14")
    .withColumn("is_savings", F.when(F.col("account_type") == "SAVINGS", 1).otherwise(0))
)
