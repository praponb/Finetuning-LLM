from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method")
    .agg(
        F.min("paid_at").alias("min_paid_at"),
        F.count(F.when(F.col("method") == "card", 1)).alias("card_count"),
    )
    .filter(F.col("card_count") < 3)
    .drop("card_count")
    .orderBy("min_paid_at", "method")
)
