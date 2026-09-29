from pyspark.sql import functions as F

result = (
    df_transactions
    .select(
        "amount",
        "is_flagged",
        F.initcap(F.trim("txn_type")).alias("txn_type_title"),
    )
    .orderBy("txn_type_title", F.col("amount").desc())
)
