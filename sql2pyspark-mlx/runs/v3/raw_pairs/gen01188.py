from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0))) / F.count("*")), 1).alias("pct_success"),
        F.min("amount").alias("min_amount"),
    )
    .filter(F.col("min_amount") > 500)
    .select("method", "pct_success", "min_amount")
    .orderBy("method")
)
