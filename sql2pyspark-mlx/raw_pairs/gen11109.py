from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        F.hour("started_at").alias("started_at_hour"),
    )
)
