from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["FAILED", "REFUNDED"]))
    .groupBy("method")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("method") == "cash", 1).otherwise(0)).alias("cash_count"),
    )
    .filter(F.col("cash_count") > 47)
    .drop("cash_count")
)
