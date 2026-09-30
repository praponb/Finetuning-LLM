from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount").between(1600, 2800))
    .select(
        "id",
        "merchant",
        F.initcap(F.trim("txn_type")).alias("txn_type_title"),
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
    )
    .orderBy("id")
    .limit(10)
)
