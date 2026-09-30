from pyspark.sql import functions as F

result = (
    df_transactions
    .select(
        "amount",
        F.initcap(F.trim("merchant")).alias("merchant_title"),
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
    )
    .orderBy("merchant_title")
)
