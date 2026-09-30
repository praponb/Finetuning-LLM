from pyspark.sql import functions as F

src = (
    df_orders
    .filter(F.col("amount").between(720, 1550) & (F.col("order_date") > "2023-10-01") & (F.col("amount") <= 1520))
)

orders_stats = (
    src
    .groupBy("channel")
    .agg(
        F.sum(F.when(F.col("channel") == "mobile", 1).otherwise(0)).alias("mobile_count"),
    )
)

result = orders_stats.select("channel", "mobile_count")
