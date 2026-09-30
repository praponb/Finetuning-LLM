from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "id",
        "browser",
        "country",
        "device",
        F.trim("device").alias("clean_device"),
    )
    .orderBy("browser", "device")
)
