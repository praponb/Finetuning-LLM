from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") < 58000)
    .select("status", F.lower("status").alias("status_lower"))
    .orderBy(F.col("status_lower").desc())
)
