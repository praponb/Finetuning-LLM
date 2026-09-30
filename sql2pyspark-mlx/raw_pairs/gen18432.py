from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") < 2100)
    .groupBy(
        F.when(F.col("amount") >= 3800, "high").when(F.col("amount") >= 1800, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.max("amount").alias("max_amount"),
        F.count("amount").alias("amount_count"),
    )
)
