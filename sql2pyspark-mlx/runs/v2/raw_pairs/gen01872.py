from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.to_date("paid_at") < "2023-11-01")
    .groupBy("method")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.max("paid_at").alias("last_paid_at"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
)
