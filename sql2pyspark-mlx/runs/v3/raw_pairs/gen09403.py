from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") > 1530)
    .groupBy(F.date_trunc("MONTH", "order_date").alias("order_month"), "channel")
    .agg(
        (F.max("shipping_fee") - F.min("shipping_fee")).alias("shipping_fee_range"),
        F.round(((100 * F.sum(F.when(F.col("status") == "SHIPPED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_shipped"),
    )
    .filter(F.col("shipping_fee_range") >= 33)
    .select("order_month", "channel", "shipping_fee_range", "pct_shipped")
)
