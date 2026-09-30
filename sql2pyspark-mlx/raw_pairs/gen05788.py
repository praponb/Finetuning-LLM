from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        F.when(F.year("order_date") < 2024, "before_2024").when(F.year("order_date") == 2024, "2024").otherwise("after_2024").alias("order_cohort"),
        "channel",
    )
    .agg(
        F.sum(F.when(F.col("channel") == "store", 1).otherwise(0)).alias("store_count"),
        F.avg("amount").alias("avg_amount"),
    )
    .filter(F.col("avg_amount") < 970)
    .drop("avg_amount")
    .orderBy(F.col("channel").desc(), "store_count", "order_cohort")
    .limit(50)
)
