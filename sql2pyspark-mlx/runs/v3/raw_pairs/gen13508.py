from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("customer_id") == 17) & F.col("shipping_fee").between(24, 26))
    .groupBy(F.date_trunc("MONTH", "order_date").alias("order_month"))
    .agg(
        F.count(F.when(F.col("channel") == "store", 1)).alias("store_count"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") >= 3)
)
