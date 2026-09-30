from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(~F.col("device").isin(["desktop", "mobile"]) | ~F.col("device").isin(["tablet", "mobile"]))
    .select(
        "id",
        "customer_id",
        "browser",
        F.concat_ws("/", "device", "country").alias("device_country_key"),
        F.initcap(F.trim("browser")).alias("browser_title"),
    )
)
