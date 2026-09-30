from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount").isNotNull())
    .groupBy("channel")
    .agg(
        F.min("order_date").alias("first_order_date"),
        F.round(((100 * F.sum(F.when(F.col("status") == "SHIPPED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_shipped"),
    )
    .orderBy("channel", "first_order_date")
)
