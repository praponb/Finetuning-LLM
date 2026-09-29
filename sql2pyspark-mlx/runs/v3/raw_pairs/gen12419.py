from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .agg(
        F.count("amount").alias("amount_count"),
        F.count("*").alias("num_transactions"),
    )
    .orderBy(F.col("num_transactions").desc())
)
