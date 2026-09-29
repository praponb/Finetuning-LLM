from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("is_flagged", "merchant")
    .agg(
        F.count("*").alias("total_count"),
        F.count("amount").alias("amount_count"),
    )
    .orderBy("total_count")
)
