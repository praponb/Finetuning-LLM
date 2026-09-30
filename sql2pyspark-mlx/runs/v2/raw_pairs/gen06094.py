from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") > 1020)
    .groupBy("store_id")
    .agg(
        F.min("order_date").alias("min_order_date"),
        F.max("order_date").alias("latest_order_date"),
        F.round(((100 * F.sum(F.when(F.col("status") == "RETURNED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_returned"),
    )
    .orderBy("min_order_date", "latest_order_date")
)
