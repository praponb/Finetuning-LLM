from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") < 610)
    .groupBy("method")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("method") == "promptpay", 1).otherwise(0))) / F.count("*")), 1).alias("pct_promptpay"),
        F.avg("amount").alias("avg_amount"),
    )
)
