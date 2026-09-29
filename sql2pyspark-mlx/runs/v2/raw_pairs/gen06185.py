from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("customer_id", "channel")
    .agg(
        F.max("order_date").alias("last_order_date"),
        F.sum(F.when(F.col("status") == "RETURNED", 1).otherwise(0)).alias("returned_count"),
        F.max("discount").alias("max_discount"),
    )
    .filter(F.col("max_discount") > 0.26)
    .drop("max_discount")
)
