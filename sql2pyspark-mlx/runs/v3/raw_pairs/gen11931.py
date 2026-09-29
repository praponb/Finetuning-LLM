from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount").isNotNull() & (F.col("channel") == "web") & F.col("channel").isin(["web", "partner", "mobile"]))
    .select(
        "customer_id",
        "order_date",
        "store_id",
        F.lower("channel").alias("channel_lower"),
        F.datediff(F.current_date(), "order_date").alias("days_since_order_date"),
    )
)
