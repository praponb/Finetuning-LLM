from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.when(F.col("amount") >= 3800, "high").when(F.col("amount") >= 1000, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.count("*").alias("num_transactions"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
    .orderBy("amount_range", "amount_bucket")
)
