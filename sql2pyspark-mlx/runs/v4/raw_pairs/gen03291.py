from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method")
    .agg(
        F.min("paid_at").alias("min_paid_at"),
        F.max("amount").alias("max_amount"),
    )
    .orderBy(F.col("max_amount").desc(), "method")
    .limit(50)
)
