from pyspark.sql import functions as F

base = (
    df_payments
    .filter(F.col("order_id").isin([2, 40]) & (F.col("order_id") == 33))
    .withColumn("clean_method", F.trim("method"))
)

method_summary = (
    base
    .groupBy("method", "status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("method") == "promptpay", 1).otherwise(0))) / F.count("*")), 1).alias("pct_promptpay"),
    )
)

result = (
    method_summary
    .filter(F.col("pct_promptpay") < 38)
    .select("method", "status", "pct_promptpay")
)
