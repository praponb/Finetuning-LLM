from pyspark.sql import functions as F

recent_payments = (
    df_payments
    .filter(F.col("amount").between(950, 1010))
    .withColumn("method_status", F.concat("method", F.lit(" "), "status"))
)

payments_stats = (
    recent_payments
    .groupBy("status")
    .agg(
        F.min("amount").alias("min_amount"),
        F.sum(F.when(F.col("method") == "cash", 1).otherwise(0)).alias("cash_count"),
    )
)

result = payments_stats.orderBy("min_amount")
