from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount").between(2900, 3100))
    .select(
        "id",
        "is_flagged",
        F.substring("txn_type", 1, 2).alias("txn_type_prefix"),
        F.initcap(F.trim("merchant")).alias("merchant_title"),
    )
)
