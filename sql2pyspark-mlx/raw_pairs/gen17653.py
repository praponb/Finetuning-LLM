from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.to_date("paid_at") < "2023-10-15") & F.col("order_id").isNull())
    .groupBy("method")
    .agg(
        F.max("paid_at").alias("latest_paid_at"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
)
