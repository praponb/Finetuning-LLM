from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method")
    .agg(
        F.min("paid_at").alias("first_paid_at"),
        F.count("*").alias("num_payments"),
    )
)
