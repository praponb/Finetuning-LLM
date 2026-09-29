from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("country").isNotNull())
    .select(
        "id",
        "country",
        F.lower("country").alias("country_lower"),
        F.datediff(F.current_date(), "started_at").alias("days_since_started_at"),
    )
)
