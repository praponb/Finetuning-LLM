from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.hour("started_at").between(9, 15))
    .select(
        "id",
        "browser",
        "device",
        "country",
        F.concat_ws("-", "country", "device").alias("country_device_key"),
        F.concat("browser", F.lit(" - "), "country").alias("browser_country"),
    )
)
