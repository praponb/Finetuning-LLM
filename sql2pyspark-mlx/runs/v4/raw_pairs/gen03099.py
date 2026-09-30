from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("browser") == "Chrome") & (F.to_date("started_at") < "2023-10-30"))
    .select(
        "browser",
        "started_at",
        "device",
        F.hour("started_at").alias("started_at_hour"),
    )
)
