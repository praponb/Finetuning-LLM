from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("country") == "JP")
    .select(
        "browser",
        "started_at",
        F.hour("started_at").alias("started_at_hour"),
        F.lower("country").alias("country_lower"),
    )
)
