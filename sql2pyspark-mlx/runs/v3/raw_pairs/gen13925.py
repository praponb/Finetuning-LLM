from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount") > 0.17)
    .groupBy(F.date_format("order_date", "yyyy-MM").alias("order_month"), "channel")
    .agg(
        F.max("discount").alias("max_discount"),
        F.max("order_date").alias("latest_order_date"),
        F.countDistinct("shipping_fee").alias("unique_customers"),
    )
    .filter(F.col("unique_customers") >= 5)
    .orderBy(F.col("max_discount").desc(), "order_month", "channel")
    .limit(50)
)
