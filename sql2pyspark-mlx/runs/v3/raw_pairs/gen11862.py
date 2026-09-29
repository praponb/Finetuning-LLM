from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") <= 660)
    .groupBy(F.hour("paid_at").alias("paid_hour"))
    .agg(
        F.count("amount").alias("amount_count"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
    )
)
