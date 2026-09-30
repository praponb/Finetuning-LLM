from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "left")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "left")
    .groupBy(
        F.when(F.col("stores.sqft") >= 15414, "high").when(F.col("stores.sqft") >= 7568, "mid").otherwise("low").alias("sqft_bucket"),
        F.col("stores.city"),
    )
    .agg(
        F.count(F.when(F.col("payments.status") == "REFUNDED", 1)).alias("refunded_count"),
        F.count("*").alias("total_count"),
    )
    .orderBy("sqft_bucket")
)
