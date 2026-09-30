from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") > 970)
    .groupBy("order_id")
    .agg(
        F.countDistinct("amount").alias("unique_orders"),
        F.min("paid_at").alias("first_paid_at"),
        F.avg("amount").alias("avg_amount"),
    )
    .filter(F.col("avg_amount") < 1250)
    .drop("avg_amount")
)
