from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .agg(
        F.sum(F.when(F.col("channel") == "mobile", 1).otherwise(0)).alias("mobile_count"),
    )
    .filter(F.col("mobile_count") >= 38)
    .select("channel", "mobile_count")
    .orderBy(F.col("mobile_count").desc(), "channel")
)
