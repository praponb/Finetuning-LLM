from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method")
    .agg(
        F.sum(F.when(F.col("method") == "cash", 1).otherwise(0)).alias("cash_count"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") < 9)
)
