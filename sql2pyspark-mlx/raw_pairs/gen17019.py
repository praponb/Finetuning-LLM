from pyspark.sql import functions as F

filtered_orders = df_orders.filter(F.col("channel") == "mobile")

customer_id_summary = (
    filtered_orders
    .groupBy("customer_id", "store_id")
    .agg(
        F.sum(F.when(F.col("status") == "COMPLETED", 1).otherwise(0)).alias("completed_count"),
    )
)

result = (
    customer_id_summary
    .select("customer_id", "store_id")
    .orderBy(F.col("store_id").desc())
)
