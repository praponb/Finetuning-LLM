from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("method") == "paypal", 1).otherwise(0))) / F.count("*")), 1).alias("pct_paypal"),
        F.round(((100 * F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0))) / F.count("*")), 1).alias("pct_success"),
    )
    .filter(F.col("pct_paypal") >= 33)
    .select("status", "pct_paypal", "pct_success")
    .orderBy(F.col("pct_success").desc(), "status")
    .limit(5)
)
