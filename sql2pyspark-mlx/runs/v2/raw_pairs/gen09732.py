from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method", "status")
    .agg(
        F.countDistinct("amount").alias("unique_statuss"),
        F.max("amount").alias("max_amount"),
    )
    .filter(F.col("unique_statuss") >= 4)
    .orderBy("unique_statuss", "method", "status")
    .limit(3)
)
