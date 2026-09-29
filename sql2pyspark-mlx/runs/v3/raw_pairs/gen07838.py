from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(((F.col("amount") > 3100) & F.col("amount").isNull()) | F.col("is_flagged"))
    .groupBy("merchant")
    .agg(
        F.min("amount").alias("min_amount"),
        F.count("amount").alias("amount_count"),
    )
)
