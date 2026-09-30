from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        "status",
        F.when(F.col("amount") >= 1250, "high").when(F.col("amount") >= 530, "mid").otherwise("low").alias("amount_bucket"),
        F.when(F.col("status").isin(["FAILED", "SUCCESS"]), "priority").otherwise("other").alias("status_group"),
    )
    .agg(F.count("*").alias("num_payments"))
)
