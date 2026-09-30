from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("device") == "desktop") & F.col("country").isin(["US", "TH"]))
    .select(
        "started_at",
        F.concat("browser", F.lit(" - "), "device").alias("browser_device"),
    )
)
