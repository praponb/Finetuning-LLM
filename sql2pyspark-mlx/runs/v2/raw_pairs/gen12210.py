from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count("amount").alias("amount_count"),
    )
)
