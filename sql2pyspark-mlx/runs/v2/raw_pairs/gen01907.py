from pyspark.sql import functions as F

src = df_transactions.filter(F.col("txn_time") < "2024-12-21")

txn_type_summary = (
    src
    .groupBy("txn_type")
    .agg(F.count(F.when(F.col("merchant") == "Shell", 1)).alias("shell_count"))
)

result = (
    txn_type_summary
    .select("txn_type", "shell_count")
    .orderBy(F.col("shell_count").desc(), "txn_type")
    .limit(3)
)
