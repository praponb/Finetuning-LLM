from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.when(F.col("amount") >= 1390, "high").when(F.col("amount") >= 340, "mid").otherwise("low").alias("amount_bucket"),
        "status",
    )
    .agg(
        F.max("amount").alias("max_amount"),
        F.sum(F.when(F.col("method") == "cash", F.col("amount")).otherwise(0)).alias("cash_amount"),
    )
)
