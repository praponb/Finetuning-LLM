from pyspark.sql import functions as F

result = (
    df_payments
    .rollup("method", "order_id")
    .agg(F.min("amount").alias("min_amount"))
    .orderBy(F.col("order_id").desc())
)
