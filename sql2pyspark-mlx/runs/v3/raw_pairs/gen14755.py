from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status") == "REFUNDED")
    .groupBy(
        F.when(F.col("amount") >= 1410, "high").when(F.col("amount") >= 640, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.sum(F.when(F.col("method") == "card", F.col("amount")).otherwise(0)).alias("card_amount"),
    )
    .filter(F.col("avg_amount") < 38)
    .select("amount_bucket", "avg_amount", "card_amount")
    .orderBy(F.col("card_amount").desc())
)
