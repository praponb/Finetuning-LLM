from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").between(21000, 30000) & (F.col("balance") > 34000) & F.col("balance").between(48000, 70000))
    .select("status", "balance", F.length("status").alias("status_length"))
    .orderBy(F.col("status_length").desc())
)
