from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("end_date") >= "2024-08-12") & (F.col("customer_id").isNotNull() | (F.col("end_date") < "2024-05-01")))
    .select(
        "id",
        "plan",
        "status",
        "monthly_fee",
        F.when(F.col("status") == "ACTIVE", 1).otherwise(0).alias("is_active"),
        F.upper("status").alias("status_upper"),
    )
)
