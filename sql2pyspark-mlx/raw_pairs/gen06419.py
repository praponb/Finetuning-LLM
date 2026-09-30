from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser").isin(["Firefox", "Chrome"]) & (F.col("device") == "mobile"))
    .select(
        "id",
        "device",
        F.concat_ws("/", "browser", "country").alias("browser_country_key"),
        F.initcap(F.trim("device")).alias("device_title"),
    )
)
