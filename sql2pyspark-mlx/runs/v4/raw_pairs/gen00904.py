from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.when(F.col("amount") >= 1460, "high").when(F.col("amount") >= 420, "mid").otherwise("low").alias("amount_bucket"),
        "method",
    )
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.count("*").alias("num_payments"),
        F.sum("amount").alias("total_amount"),
    )
    .orderBy("num_payments", "amount_bucket", "method")
    .limit(50)
)
