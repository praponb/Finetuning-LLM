from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("order_id", F.year("paid_at").alias("paid_year"))
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
    .orderBy(F.col("order_id").desc())
)
