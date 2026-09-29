from pyspark.sql import functions as F

result = (
    df_orders
    .filter(((F.col("amount") >= 1190) & (F.col("status") != "COMPLETED")) | F.col("customer_id").isNull())
    .groupBy("status", F.date_format("order_date", "yyyy-MM").alias("order_month"))
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.min("order_date").alias("min_order_date"),
        F.max("order_date").alias("last_order_date"),
    )
    .filter(F.col("avg_amount") >= 72)
    .orderBy(F.col("last_order_date").desc(), "status")
)
