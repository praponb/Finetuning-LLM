from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([19, 27]) & (F.col("amount") >= 2100))
    .select(
        "id",
        "is_flagged",
        F.initcap(F.trim("merchant")).alias("merchant_title"),
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
    )
    .orderBy(F.col("is_flagged_flag").desc())
)
