from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("is_flagged"))
    .groupBy("merchant")
    .agg(
        F.sum(F.when(F.col("merchant") == "Starbucks", F.col("amount")).otherwise(0)).alias("starbucks_amount"),
    )
    .orderBy(F.col("starbucks_amount").desc(), F.col("merchant").desc())
    .limit(3)
)
