from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("discount") > 0.23) & (F.year("order_date") >= 2024) & (F.col("discount") <= 0.27))
    .groupBy(
        F.date_trunc("MONTH", "order_date").alias("order_month"),
        "channel",
        "status",
    )
    .agg(
        F.sum(F.when(F.col("status") == "SHIPPED", 1).otherwise(0)).alias("shipped_count"),
        F.max("amount").alias("max_amount"),
        F.round(((100 * F.sum(F.when(F.col("status") == "RETURNED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_returned"),
    )
)
