from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        F.when(F.col("amount") >= 1260, "high").when(F.col("amount") >= 640, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("channel") == "mobile", F.col("amount")).otherwise(0)).alias("mobile_amount"),
    )
    .filter(F.col("mobile_amount") >= 46)
)
