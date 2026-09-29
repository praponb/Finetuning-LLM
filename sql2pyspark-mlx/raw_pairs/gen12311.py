from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.hour("started_at").between(7, 12) | F.col("device").isin(["tablet", "desktop", "mobile"]))
    .select(
        "browser",
        "started_at",
        "device",
        F.to_date("started_at").alias("started_at_date"),
        F.upper("device").alias("device_upper"),
    )
)
