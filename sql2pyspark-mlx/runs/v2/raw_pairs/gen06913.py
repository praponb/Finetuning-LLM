from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("device") == "desktop") & (F.col("device") != "mobile"))
    .select(
        "customer_id",
        "browser",
        F.date_format("started_at", "yyyy-MM").alias("started_at_label"),
        F.upper("device").alias("device_upper"),
    )
)
