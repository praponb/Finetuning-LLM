from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method")
    .agg(
        F.count("amount").alias("amount_count"),
        F.round(((100 * F.sum(F.when(F.col("method") == "promptpay", 1).otherwise(0))) / F.count("*")), 1).alias("pct_promptpay"),
    )
    .filter(F.col("amount_count") < 8)
    .orderBy(F.col("pct_promptpay").desc(), F.col("amount_count").desc())
)
