from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNull() & (F.col("amount") > 610) & F.col("amount").between(870, 1250))
    .groupBy("status")
    .agg(F.min("amount").alias("min_amount"))
    .filter(F.col("min_amount") > 1520)
)
