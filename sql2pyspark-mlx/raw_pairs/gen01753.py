from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNotNull() & F.col("order_id").isNull())
    .groupBy("method")
    .agg(F.min("amount").alias("min_amount"))
)
