from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method")
    .agg(
        F.max("amount").alias("max_amount"),
        F.max("paid_at").alias("last_paid_at"),
    )
    .filter(F.col("max_amount") > 600)
    .select("method", "max_amount", "last_paid_at")
)
