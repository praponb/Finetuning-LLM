from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("device").isin(["mobile", "tablet"]))
    .select(
        "device",
        "customer_id",
        F.length("browser").alias("browser_length"),
        F.datediff(F.current_date(), "started_at").alias("days_since_started_at"),
    )
)
