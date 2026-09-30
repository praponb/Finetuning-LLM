from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status") == "SUCCESS")
    .groupBy(
        F.when(F.col("amount") >= 1290, "high").when(F.col("amount") >= 440, "mid").otherwise("low").alias("amount_bucket"),
        "method",
    )
    .agg(
        F.count("*").alias("num_payments"),
        F.min("amount").alias("min_amount"),
    )
    .filter(F.col("num_payments") >= 8)
    .select("amount_bucket", "method", "num_payments", "min_amount")
    .orderBy("amount_bucket")
)
