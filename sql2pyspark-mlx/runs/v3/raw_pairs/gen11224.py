from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("method") == "paypal", 1).otherwise(0)).alias("paypal_count"),
        F.countDistinct("amount").alias("unique_statuss"),
    )
    .filter(F.col("unique_statuss") >= 3)
    .drop("unique_statuss")
    .orderBy(F.col("status").desc(), "paypal_count")
    .limit(3)
)
