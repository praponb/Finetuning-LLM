from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id") == 4)
    .groupBy("status")
    .agg(
        F.countDistinct("amount").alias("unique_methods"),
        F.min("amount").alias("min_amount"),
    )
)
