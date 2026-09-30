from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status", F.month("order_date").alias("order_month"), "channel")
    .agg(
        F.sum(F.when(F.col("status") == "PENDING", F.col("amount")).otherwise(0)).alias("pending_amount"),
    )
)
