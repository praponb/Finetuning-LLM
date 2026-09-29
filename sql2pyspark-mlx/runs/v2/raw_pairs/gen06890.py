from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNotNull() & (F.col("amount") < 500))
    .agg(
        F.count("amount").alias("amount_count"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
)
