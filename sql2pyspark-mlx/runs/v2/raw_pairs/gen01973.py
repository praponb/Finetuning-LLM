from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("customer_id")
    .agg(F.min("balance").alias("min_balance"))
    .orderBy(F.col("min_balance").desc())
)
