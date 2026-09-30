from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("status") == "REFUNDED") & F.col("amount").between(460, 780) & F.col("order_id").isin([3, 12, 21]))
    .groupBy("status")
    .agg(
        F.min("amount").alias("min_amount"),
        F.count("amount").alias("amount_count"),
    )
)
