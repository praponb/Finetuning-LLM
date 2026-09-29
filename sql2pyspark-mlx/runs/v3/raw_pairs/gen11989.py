from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.count("amount").alias("amount_count"),
    )
    .filter(F.col("amount_count") < 3)
)
