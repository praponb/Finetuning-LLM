from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .pivot("status", ["ACTIVE", "CANCELLED"])
    .agg(F.sum("monthly_fee").alias("amount"), F.count(F.lit(1)).alias("item_count"))
    .select(
        "plan",
        F.col("ACTIVE_amount").alias("active_amount"),
        F.col("ACTIVE_item_count").alias("active_item_count"),
        F.col("CANCELLED_amount").alias("cancelled_amount"),
        F.col("CANCELLED_item_count").alias("cancelled_item_count"),
    )
)
