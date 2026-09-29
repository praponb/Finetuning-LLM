from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.date_trunc("MONTH", "order_date").alias("order_month"))
    .agg(
        F.count("*").alias("total_count"),
        F.sum(F.when(F.col("status") == "SHIPPED", 1).otherwise(0)).alias("shipped_count"),
    )
    .orderBy("order_month")
    .limit(50)
)
