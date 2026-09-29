from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.when(F.col("amount") >= 1270, "high").when(F.col("amount") >= 720, "mid").otherwise("low").alias("amount_bucket"),
        "status",
    )
    .agg(F.count("amount").alias("amount_count"))
    .filter(F.col("amount_count") >= 8)
    .select("amount_bucket", "status", "amount_count")
)
