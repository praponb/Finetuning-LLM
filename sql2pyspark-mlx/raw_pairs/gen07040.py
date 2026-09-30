from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("status") == "SUCCESS") & (F.col("amount") > 1320))
    .groupBy("status", "method", "order_id")
    .agg(
        F.countDistinct("amount").alias("unique_statuss"),
        F.min("paid_at").alias("first_paid_at"),
    )
    .orderBy("method")
)
