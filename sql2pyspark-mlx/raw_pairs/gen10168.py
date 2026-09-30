from pyspark.sql import functions as F

recent_orders = (
    df_orders
    .filter((F.col("discount") <= 0.32) & ~F.col("status").isin(["CANCELLED", "SHIPPED", "COMPLETED"]))
)

status_summary = (
    recent_orders
    .groupBy("status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("channel") == "store", 1).otherwise(0))) / F.count("*")), 1).alias("pct_store"),
        F.min("order_date").alias("min_order_date"),
    )
)

result = (
    status_summary
    .select("status", "pct_store", "min_order_date")
    .orderBy(F.col("status").desc(), F.col("pct_store").desc())
)
