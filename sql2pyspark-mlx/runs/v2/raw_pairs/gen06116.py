from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("status") == "EXPIRED") | ~F.col("status").isin(["ACTIVE", "PAUSED", "CANCELLED"]))
    .select(
        "id",
        "end_date",
        "start_date",
        "status",
        F.date_add("end_date", 6).alias("end_date_plus_6d"),
        F.upper("status").alias("status_upper"),
    )
)
