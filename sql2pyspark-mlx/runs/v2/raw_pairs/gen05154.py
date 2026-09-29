from pyspark.sql import functions as F

result = (
    df_payments
    .filter(((F.col("status") == "SUCCESS") & (F.col("paid_at") >= "2024-04-20")) | ~F.col("status").isin(["REFUNDED", "SUCCESS", "FAILED"]))
    .agg(
        F.countDistinct("amount").alias("unique_methods"),
        F.min("paid_at").alias("min_paid_at"),
    )
    .orderBy("unique_methods")
)
