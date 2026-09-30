from pyspark.sql import functions as F

result = (
    df_payments
    .select("method", "order_id", "status", F.floor("amount").alias("amount_whole"))
    .orderBy(F.col("order_id").desc())
)
