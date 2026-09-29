from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status", "order_id")
    .agg(F.min("amount").alias("min_amount"))
    .orderBy(F.col("min_amount").desc())
)
