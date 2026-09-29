from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") >= 760) & (F.col("status") == "REFUNDED"))
    .groupBy("method")
    .agg(F.min("paid_at").alias("min_paid_at"))
)
