from pyspark.sql import functions as F

base = (
    df_transactions
    .filter((F.col("amount") > 2500) & ((F.col("merchant") == "7-Eleven") | (F.col("txn_type") == "FEE")))
)

txn_type_summary = (
    base
    .groupBy("txn_type")
    .agg(F.max("amount").alias("max_amount"))
)

result = (
    txn_type_summary
    .filter(F.col("max_amount") > 2400)
    .select("txn_type", "max_amount")
    .orderBy(F.col("max_amount").desc())
)
