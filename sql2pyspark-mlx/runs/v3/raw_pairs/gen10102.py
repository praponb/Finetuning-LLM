from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.year("opened_date") >= 2023) & (F.col("balance") >= 50000))
    .select("status", F.trim("status").alias("clean_status"))
    .orderBy("clean_status", F.col("status").desc())
)
