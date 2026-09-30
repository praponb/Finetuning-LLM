from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") >= 750)
    .groupBy(
        F.date_trunc("MONTH", "order_date").alias("order_month"),
        "channel",
        "status",
    )
    .agg(F.count(F.when(F.col("channel") == "mobile", 1)).alias("mobile_count"))
    .orderBy("mobile_count")
)
