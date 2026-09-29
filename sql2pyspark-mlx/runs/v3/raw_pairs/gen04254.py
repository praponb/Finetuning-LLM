from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status").isin(["PENDING", "SHIPPED", "CANCELLED"]))
    .groupBy(F.date_trunc("MONTH", "order_date").alias("order_month"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "COMPLETED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_completed"),
    )
)
