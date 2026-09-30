from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("customer_id", "channel", "status")
    .agg(
        F.min("order_date").alias("min_order_date"),
        F.sum(F.when(F.col("status") == "COMPLETED", F.col("amount")).otherwise(0)).alias("completed_amount"),
    )
)
