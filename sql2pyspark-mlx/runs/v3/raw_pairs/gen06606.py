from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan") == "enterprise")
    .select(
        "customer_id",
        F.date_trunc("year", "start_date").alias("start_date_period"),
        F.when(F.col("plan") == "basic", 1).otherwise(0).alias("is_basic"),
    )
)
