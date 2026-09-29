from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("method") == "promptpay", 1).otherwise(0))) / F.count("*")), 1).alias("pct_promptpay"),
    )
    .filter(F.col("pct_promptpay") < 34)
)
