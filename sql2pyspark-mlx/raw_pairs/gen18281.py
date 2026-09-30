from pyspark.sql import functions as F

recent_orders = (
    df_orders
    .filter(((F.col("channel") == "web") & F.col("customer_id").isNull()) | F.col("store_id").isNull())
    .withColumn("amount_with_shipping", (F.col("amount") - F.col("shipping_fee")))
)

order_month_summary = (
    recent_orders
    .groupBy(
        F.date_format("order_date", "yyyy-MM").alias("order_month"),
        F.year("order_date").alias("order_year"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "RETURNED", 1).otherwise(0)).alias("returned_count"),
    )
)

result = order_month_summary.orderBy("returned_count")
