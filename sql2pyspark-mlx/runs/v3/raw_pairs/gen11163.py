from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isin([4, 5, 26]) & (F.year("order_date") >= 2024))
    .groupBy("channel", F.date_trunc("MONTH", "order_date").alias("order_month"))
    .agg(
        F.count("amount").alias("amount_count"),
        F.round(F.avg("discount"), 2).alias("avg_discount"),
    )
    .orderBy(F.col("avg_discount").desc(), "channel", "order_month")
    .limit(10)
)
