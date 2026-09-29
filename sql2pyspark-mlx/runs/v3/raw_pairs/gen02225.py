from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status").isin(["PENDING", "COMPLETED", "CANCELLED"]) & (F.col("customer_id") == 36) & F.col("discount").isNull())
    .groupBy(
        "channel",
        F.date_format("order_date", "yyyy-MM").alias("order_month"),
        F.year("order_date").alias("order_year"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "RETURNED", F.col("amount")).otherwise(0)).alias("returned_amount"),
        F.round(((100 * F.sum(F.when(F.col("channel") == "web", 1).otherwise(0))) / F.count("*")), 1).alias("pct_web"),
    )
)
