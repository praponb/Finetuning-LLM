from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([12, 27, 34, 40]) & (F.col("order_id").isNotNull() | (F.col("amount") >= 880)))
    .select("id", "method", F.upper("method").alias("method_upper"))
)
