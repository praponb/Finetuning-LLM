from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel") == "mobile")
    .groupBy(F.month("order_date").alias("order_month"))
    .agg(
        F.sum(F.when(F.col("channel") == "store", 1).otherwise(0)).alias("store_count"),
    )
    .filter(F.col("store_count") < 47)
    .select("order_month", "store_count")
    .orderBy(F.col("order_month").desc())
)
