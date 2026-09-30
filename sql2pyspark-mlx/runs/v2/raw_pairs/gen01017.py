from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.date_format("order_date", "yyyy-MM").alias("order_month"))
    .agg(
        F.max("order_date").alias("latest_order_date"),
        F.sum(F.when(F.col("status") == "CANCELLED", F.col("amount")).otherwise(0)).alias("cancelled_amount"),
    )
    .filter(F.col("cancelled_amount") > 56)
    .select("order_month", "latest_order_date", "cancelled_amount")
    .orderBy(F.col("latest_order_date").desc(), "order_month")
    .limit(20)
)
