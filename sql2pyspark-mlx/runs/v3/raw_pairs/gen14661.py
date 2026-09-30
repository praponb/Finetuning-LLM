from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.when(F.col("amount") >= 1430, "high").when(F.col("amount") >= 460, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.count("*").alias("cnt"))
    .orderBy("amount_bucket")
)
