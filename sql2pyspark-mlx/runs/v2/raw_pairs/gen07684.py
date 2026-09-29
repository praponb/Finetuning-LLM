from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") < 770)
    .groupBy(
        F.when(F.col("amount") >= 1400, "high").when(F.col("amount") >= 690, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0))) / F.count("*")), 1).alias("pct_success"),
    )
    .filter(F.col("pct_success") >= 68)
)
