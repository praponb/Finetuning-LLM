from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") <= 1200) & (F.col("status") == "SUCCESS"))
    .withColumn("status_lower", F.lower("status"))
)
