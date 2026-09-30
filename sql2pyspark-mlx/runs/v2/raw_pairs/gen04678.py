from pyspark.sql import functions as F

filtered_orders = (
    df_orders
    .filter(F.col("amount").between(470, 940) & ((F.col("status") == "CANCELLED") | F.col("store_id").isNotNull()))
)

summary = (
    filtered_orders
    .groupBy(F.date_trunc("MONTH", "order_date").alias("order_month"))
    .agg(F.round(F.avg("discount"), 2).alias("avg_discount"))
)

result = (
    summary
    .select("order_month", "avg_discount")
    .orderBy(F.col("avg_discount").desc(), "order_month")
)
