from pyspark.sql import functions as F

base = df_transactions.filter(~(~F.col("is_flagged") & ~F.col("is_flagged")))

summary = (
    base
    .groupBy("merchant", "account_id")
    .agg(F.count(F.when(F.col("txn_type") == "CREDIT", 1)).alias("credit_count"))
)

result = (
    summary
    .filter(F.col("credit_count") < 4)
    .select("merchant", "credit_count")
    .orderBy("merchant", F.col("credit_count").desc())
)
