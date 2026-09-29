from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([13, 15, 37, 38]) & ((F.col("paid_at") >= "2024-02-01") | F.col("order_id").isin([18, 22, 27, 29])))
    .groupBy("method")
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.round(((100 * F.sum(F.when(F.col("method") == "cash", 1).otherwise(0))) / F.count("*")), 1).alias("pct_cash"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
    .orderBy(F.col("pct_cash").desc())
)
