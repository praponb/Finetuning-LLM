from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") >= 560)
    .groupBy("status", "method")
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.countDistinct("amount").alias("unique_statuss"),
    )
    .filter(F.col("avg_amount") < 1060)
    .orderBy(F.col("avg_amount").desc())
)
