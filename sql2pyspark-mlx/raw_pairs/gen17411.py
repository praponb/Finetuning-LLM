from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.when(F.col("amount") >= 1590, "high").when(F.col("amount") >= 430, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("method") == "promptpay", 1).otherwise(0))) / F.count("*")), 1).alias("pct_promptpay"),
        F.min("amount").alias("min_amount"),
    )
    .orderBy("amount_bucket")
)
