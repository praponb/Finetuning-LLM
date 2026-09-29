from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        "channel",
        F.when(F.col("discount") >= 0.41, "PREMIUM").otherwise("STANDARD").alias("discount_tier"),
        "customer_id",
    )
    .agg(
        F.count(F.when(F.col("status") == "COMPLETED", 1)).alias("completed_rows"),
        F.count_if(F.col("channel") == "web").alias("web_events"),
    )
    .orderBy("completed_rows")
)
