from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.when(F.col("method").isin(["card", "promptpay"]), "priority").otherwise("other").alias("method_group"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("method") == "cash", 1).otherwise(0))) / F.count("*")), 1).alias("pct_cash"),
    )
    .filter(F.col("pct_cash") >= 27)
    .orderBy("pct_cash", "method_group")
    .limit(5)
)
