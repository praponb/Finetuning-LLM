from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(F.hour("paid_at").alias("paid_hour"))
    .agg(
        F.count("amount").alias("amount_count"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
)
