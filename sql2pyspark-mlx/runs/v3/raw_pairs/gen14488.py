from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(F.to_date("paid_at").alias("paid_day"))
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
    .filter(F.col("avg_amount") > 43)
    .orderBy("paid_day")
)
