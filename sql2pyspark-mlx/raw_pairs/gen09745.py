from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(
        "id",
        "status",
        "end_date",
        "start_date",
        F.when(F.col("status") == "ACTIVE", 1).otherwise(0).alias("is_active"),
        F.concat("status", F.lit("/"), "plan").alias("status_plan"),
    )
)
