from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") <= 3100)
    .select(
        "amount",
        "account_id",
        F.when(F.col("merchant") == "Amazon", 1).otherwise(0).alias("is_amazon"),
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
    )
    .orderBy("account_id")
)
