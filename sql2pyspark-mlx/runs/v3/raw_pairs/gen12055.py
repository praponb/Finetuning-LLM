from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status") != "REFUNDED")
    .groupBy(
        F.when(F.col("amount") >= 1280, "high").when(F.col("amount") >= 490, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("method") == "card", 1).otherwise(0))) / F.count("*")), 1).alias("pct_card"),
    )
    .orderBy(F.col("pct_card").desc(), F.col("amount_bucket").desc())
    .limit(1)
)
