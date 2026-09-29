from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount") < 0.22)
    .groupBy("status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "RETURNED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_returned"),
        F.max("order_date").alias("latest_order_date"),
    )
    .filter(F.col("pct_returned") > 31)
    .orderBy("status", F.col("latest_order_date").desc())
)
