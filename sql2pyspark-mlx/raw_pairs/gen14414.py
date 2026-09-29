from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.hour("paid_at").between(12, 20))
    .groupBy("method")
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.round(((100 * F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0))) / F.count("*")), 1).alias("pct_success"),
    )
    .filter(F.col("avg_amount") < 1590)
    .orderBy("method")
    .limit(1)
)
