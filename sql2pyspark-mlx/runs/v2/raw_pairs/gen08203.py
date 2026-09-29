from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") <= 960)
    .groupBy("status")
    .agg(
        F.min("paid_at").alias("first_paid_at"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.round(((100 * F.sum(F.when(F.col("method") == "card", 1).otherwise(0))) / F.count("*")), 1).alias("pct_card"),
    )
)
