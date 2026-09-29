from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("merchant") == "7-Eleven") | (F.col("merchant") != "Lazada"))
    .groupBy(
        F.when(F.col("amount") >= 3200, "high").when(F.col("amount") >= 1500, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("merchant") == "7-Eleven", F.col("amount")).otherwise(0)).alias("7_eleven_amount"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
    .filter(F.col("avg_amount") < 45)
    .orderBy(F.col("avg_amount").desc())
)
