from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .agg(
        F.max("order_date").alias("latest_order_date"),
        F.round(((100 * F.sum(F.when(F.col("status") == "SHIPPED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_shipped"),
    )
    .orderBy(F.col("latest_order_date").desc())
)
