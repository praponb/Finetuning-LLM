from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") <= 52000)
    .select("id", "customer_id", F.trim("status").alias("clean_status"))
    .orderBy(F.col("id").desc())
)
