from pyspark.sql import functions as F

filtered_orders = (
    df_orders
    .filter(F.col("channel").isin(["partner", "mobile", "web"]) & (F.col("status") == "PENDING"))
)

agg = (
    filtered_orders
    .groupBy(F.month("order_date").alias("order_month"))
    .agg(
        F.min("order_date").alias("first_order_date"),
        F.max("order_date").alias("latest_order_date"),
    )
)

result = agg.orderBy(F.col("order_month").desc(), "latest_order_date")
