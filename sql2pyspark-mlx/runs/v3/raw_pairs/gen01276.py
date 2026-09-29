from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.when(F.col("amount") >= 1480, "high").when(F.col("amount") >= 350, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.count(F.when(F.col("method") == "card", 1)).alias("card_count"),
        F.count("amount").alias("amount_count"),
    )
)
