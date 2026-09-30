from pyspark.sql import functions as F

recent_orders = df_orders.filter(F.col("customer_id").isNull() & (F.col("status") == "PENDING"))

summary = (
    recent_orders
    .groupBy("channel")
    .agg(F.count("discount").alias("discount_count"))
)

result = (
    summary
    .select("channel", "discount_count")
    .orderBy(F.col("discount_count").desc())
)
