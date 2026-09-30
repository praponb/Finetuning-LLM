from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel", F.date_format("order_date", "yyyy-MM").alias("order_month"))
    .agg(
        F.sum(F.when(F.col("channel") == "mobile", F.col("discount")).otherwise(0)).alias("mobile_discount"),
    )
    .filter(F.col("mobile_discount") > 43)
    .select("channel", "order_month", "mobile_discount")
)
