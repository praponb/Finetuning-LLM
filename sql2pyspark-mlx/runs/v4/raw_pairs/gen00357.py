from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount") <= 0.39)
    .groupBy(F.date_format("order_date", "yyyy-MM").alias("order_month"))
    .agg(
        F.sum(F.when(F.col("status") == "SHIPPED", F.col("shipping_fee")).otherwise(0)).alias("shipped_shipping_fee"),
    )
    .filter(F.col("shipped_shipping_fee") > 72)
)
