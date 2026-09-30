from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("is_flagged")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.count("*").alias("row_count"),
    )
)
