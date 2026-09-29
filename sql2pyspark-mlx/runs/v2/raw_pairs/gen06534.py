from pyspark.sql import functions as F

valid_orders = (
    df_orders
    .filter(F.col("status") == "PENDING")
    .withColumn("shipping_fee_minus_discount", (F.col("shipping_fee") - F.col("discount")))
)

orders_stats = (
    valid_orders
    .groupBy(F.date_trunc("MONTH", "order_date").alias("order_month"), "channel")
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("amount").alias("unique_statuss"),
    )
)

result = (
    orders_stats
    .filter(F.col("unique_statuss") < 9)
    .select("order_month", "channel", "cnt")
    .orderBy(F.col("cnt").desc())
)
