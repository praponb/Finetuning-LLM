from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id") == 27)
    .groupBy("method", "status")
    .agg(
        F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0)).alias("success_count"),
        F.round(((100 * F.sum(F.when(F.col("method") == "card", 1).otherwise(0))) / F.count("*")), 1).alias("pct_card"),
    )
    .orderBy(F.col("success_count").desc(), "method", "status")
    .limit(20)
)
