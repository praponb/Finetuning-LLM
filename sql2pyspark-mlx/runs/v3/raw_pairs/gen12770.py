from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("started_at") >= "2023-11-01") & ~F.col("device").isin(["tablet", "mobile", "desktop"]))
    .select(
        "id",
        "device",
        F.concat("country", F.lit("/"), "browser").alias("country_browser"),
        F.concat_ws("-", "device", "country").alias("device_country_key"),
    )
)
