from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "id",
        "customer_id",
        "started_at",
        F.lower("country").alias("country_lower"),
        F.hour("started_at").alias("started_at_hour"),
    )
)
