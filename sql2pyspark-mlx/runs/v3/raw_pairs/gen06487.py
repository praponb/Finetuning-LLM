from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~F.col("method").isin(["promptpay", "cash"]))
    .groupBy(
        "status",
        F.when(F.col("amount") >= 1540, "high").when(F.col("amount") >= 550, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "REFUNDED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_refunded"),
    )
)
