from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method", F.year("paid_at").alias("paid_year"))
    .agg(
        F.sum(F.when(F.col("method") == "card", 1).otherwise(0)).alias("card_count"),
        F.sum(F.when(F.col("status") == "FAILED", F.col("amount")).otherwise(0)).alias("failed_amount"),
    )
    .filter(F.col("card_count") >= 74)
)
