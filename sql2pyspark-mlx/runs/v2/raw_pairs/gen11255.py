from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.when(F.col("amount") >= 1710, "high").when(F.col("amount") >= 550, "mid").otherwise("low").alias("amount_bucket"),
        "status",
    )
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.count("*").alias("num_payments"),
    )
    .filter(F.col("num_payments") > 2)
    .drop("num_payments")
    .orderBy("amount_bucket")
)
