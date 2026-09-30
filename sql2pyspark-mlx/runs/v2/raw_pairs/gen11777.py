from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status") == "RETURNED")
    .groupBy(
        "channel",
        F.date_format("order_date", "yyyy-MM").alias("order_month"),
        "store_id",
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("channel") == "mobile", 1).otherwise(0))) / F.count("*")), 1).alias("pct_mobile"),
    )
    .orderBy("order_month")
)
