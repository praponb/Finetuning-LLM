from pyspark.sql import functions as F

valid_orders = (
    df_orders
    .filter(~(((F.col("channel") == "store") & F.col("status").isin(["CANCELLED", "PENDING"])) | (F.col("shipping_fee") <= 34)))
)

orders_stats = (
    valid_orders
    .groupBy(F.date_trunc("MONTH", "order_date").alias("order_month"), "status")
    .agg(
        F.min("order_date").alias("min_order_date"),
        F.avg("discount").alias("avg_discount"),
    )
)

result = orders_stats.select("order_month", "status", "min_order_date", "avg_discount")
