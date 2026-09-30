from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([2, 37]) & F.col("amount").between(3400, 3800) & F.col("is_flagged"))
    .select("id", "account_id", F.upper("txn_type").alias("txn_type_upper"))
    .orderBy(F.col("account_id").desc())
)
