from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") >= 940)
    .groupBy(
        F.when(F.col("status").isin(["PENDING", "RETURNED"]), "priority").otherwise("other").alias("status_group"),
        F.date_trunc("MONTH", "order_date").alias("order_month"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("channel") == "web", 1).otherwise(0))) / F.count("*")), 1).alias("pct_web"),
    )
    .filter(F.col("pct_web") >= 59)
)
