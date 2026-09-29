from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.date_trunc("MONTH", "order_date").alias("order_month"))
    .agg(
        (F.max("shipping_fee") - F.min("shipping_fee")).alias("shipping_fee_range"),
        F.round(((100 * F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_cancelled"),
    )
    .filter(F.col("pct_cancelled") >= 30)
)
