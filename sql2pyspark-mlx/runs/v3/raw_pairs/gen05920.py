from pyspark.sql import functions as F

filtered_accounts = (
    df_accounts
    .filter((F.col("opened_date") < "2025-02-01") & (F.col("balance") > 49000))
    .withColumn("abs_balance", F.abs("balance"))
)

agg = (
    filtered_accounts
    .groupBy("status")
    .agg(F.min("opened_date").alias("first_opened_date"))
)

result = agg.select("status", "first_opened_date")
