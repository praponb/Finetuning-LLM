from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id") == 27)
    .groupBy(
        F.when(F.col("amount") >= 1270, "high").when(F.col("amount") >= 670, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.max("amount").alias("max_amount"))
    .orderBy("amount_bucket")
)
