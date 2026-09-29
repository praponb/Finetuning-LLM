from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("customer_id")
    .agg(
        F.max("order_date").alias("last_order_date"),
        F.count(F.when(F.col("status") == "SHIPPED", 1)).alias("shipped_count"),
    )
    .filter(F.col("shipped_count") < 2)
    .orderBy(F.col("customer_id").desc())
    .limit(1)
)
