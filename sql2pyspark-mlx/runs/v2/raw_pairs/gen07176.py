from pyspark.sql import functions as F

src = (
    df_payments
    .filter(F.col("order_id").isin([20, 33, 37]) & F.col("status").isin(["FAILED", "SUCCESS", "REFUNDED"]))
)

summary = (
    src
    .groupBy(
        F.when(F.col("amount") >= 1260, "high").when(F.col("amount") >= 750, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.countDistinct("amount").alias("unique_methods"))
)

result = (
    summary
    .filter(F.col("unique_methods") < 5)
    .select("amount_bucket", "unique_methods")
    .orderBy("amount_bucket")
)
