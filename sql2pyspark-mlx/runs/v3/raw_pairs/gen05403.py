from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("txn_time") >= "2023-10-01") & F.col("is_flagged"))
    .select("id", "account_id", F.coalesce("amount", F.lit(0)).alias("amount_filled"))
    .orderBy(F.col("account_id").desc())
)
