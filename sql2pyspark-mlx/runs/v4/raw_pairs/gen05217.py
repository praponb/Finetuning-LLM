from pyspark.sql import functions as F

valid_orders = (
    df_orders
    .filter((F.col("discount") <= 0.17) | ~F.col("channel").isin(["web", "store"]))
)

orders_stats = (
    valid_orders
    .groupBy(F.month("order_date").alias("order_month"))
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.sum(F.when(F.col("channel") == "partner", 1).otherwise(0)).alias("partner_count"),
    )
)

result = orders_stats.orderBy(F.col("partner_count").desc(), F.col("avg_amount").desc())
