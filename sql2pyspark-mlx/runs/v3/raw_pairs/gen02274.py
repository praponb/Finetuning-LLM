from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(((F.col("started_at") < "2023-11-25") & (F.col("device") == "tablet")) | (F.col("started_at") < "2025-10-01"))
    .select(
        "id",
        "country",
        "device",
        "customer_id",
        F.datediff(F.current_date(), "started_at").alias("days_since_started_at"),
    )
)
