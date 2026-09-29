from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.when(F.col("amount") >= 4200, "high").when(F.col("amount") >= 1600, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.count("amount").alias("amount_count"),
        F.count("*").alias("row_count"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
)
