from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") >= 940)
    .groupBy(F.date_trunc("MONTH", "order_date").alias("order_month"))
    .agg(
        (F.max("discount") - F.min("discount")).alias("discount_range"),
        F.max("order_date").alias("last_order_date"),
        F.count("*").alias("order_count"),
    )
    .filter(F.col("order_count") > 5)
    .drop("order_count")
)
