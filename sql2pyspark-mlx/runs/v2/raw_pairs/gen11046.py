from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.hour("started_at").between(8, 14))
    .select(
        "started_at",
        F.concat_ws("/", "device", "browser").alias("device_browser_key"),
        F.col("started_at").cast("date").alias("started_at_day"),
    )
)
