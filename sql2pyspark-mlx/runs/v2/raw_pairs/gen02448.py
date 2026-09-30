from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isin(["store", "mobile", "partner"]) & (F.col("customer_id") == 30))
    .groupBy("status", "channel")
    .agg(
        F.countDistinct("amount").alias("unique_channels"),
        F.min("amount").alias("min_amount"),
    )
    .orderBy(F.col("min_amount").desc(), F.col("unique_channels").desc())
)
