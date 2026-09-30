from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.month("order_date").alias("order_month"), "channel")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "PENDING", 1).otherwise(0))) / F.count("*")), 1).alias("pct_pending"),
    )
    .filter(F.col("pct_pending") < 40)
    .select("order_month", "channel", "pct_pending")
)
