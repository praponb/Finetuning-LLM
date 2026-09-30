from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") >= 640) & (F.col("order_id") == 10) & F.col("amount").between(670, 1240))
    .groupBy("status")
    .agg(
        F.count("*").alias("row_count"),
        F.min("paid_at").alias("first_paid_at"),
    )
)
