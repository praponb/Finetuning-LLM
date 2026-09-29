from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status", "order_id")
    .agg(
        F.min("amount").alias("min_amount"),
        F.avg("amount").alias("avg_amount"),
        F.max("amount").alias("max_amount"),
    )
)
