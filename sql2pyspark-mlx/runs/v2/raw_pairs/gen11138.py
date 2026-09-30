from pyspark.sql import functions as F

valid_orders = (
    df_orders
    .filter(F.col("discount").between(0.17, 0.2) & ((F.col("shipping_fee") >= 15) | (F.col("shipping_fee") < 25)))
    .withColumn("order_date_plus_months", F.add_months("order_date", 12))
)

summary = (
    valid_orders
    .groupBy(
        "status",
        F.date_format("order_date_plus_months", "yyyy-MM").alias("order_plus_months_month"),
    )
    .agg(F.min("shipping_fee").alias("min_shipping_fee"))
)

result = (
    summary
    .select("status", "order_plus_months_month", "min_shipping_fee")
    .orderBy("status", "order_plus_months_month")
    .limit(5)
)
