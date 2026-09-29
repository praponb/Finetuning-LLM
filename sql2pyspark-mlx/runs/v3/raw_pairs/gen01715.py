from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan") == "standard")
    .select(
        "status",
        F.when(F.col("plan") == "basic", 1).otherwise(0).alias("is_basic"),
        F.date_trunc("month", "start_date").alias("start_date_period"),
    )
)
