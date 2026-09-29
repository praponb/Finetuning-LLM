from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged") | (F.col("amount") < 1100))
    .agg(
        F.count("amount").alias("amount_count"),
        F.sum("amount").alias("total_amount"),
    )
    .orderBy(F.col("total_amount").desc())
)
