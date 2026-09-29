from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        "order_id",
        "status",
        F.when(F.col("amount") >= 1700, "high").when(F.col("amount") >= 390, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.countDistinct("amount").alias("unique_statuss"),
    )
    .filter(F.col("unique_statuss") >= 1)
    .orderBy(F.col("unique_statuss").desc(), "order_id", "status", "amount_bucket")
    .limit(10)
)
