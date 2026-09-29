from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "id",
        "device",
        F.upper("browser").alias("browser_upper"),
        F.length("browser").alias("browser_length"),
    )
    .orderBy("device", "browser_upper")
)
