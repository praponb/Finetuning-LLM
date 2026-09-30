from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("device").isin(["tablet", "mobile", "desktop"]))
    .select(
        "id",
        "device",
        F.trim("device").alias("clean_device"),
        F.concat("device", F.lit(" "), "country").alias("device_country"),
    )
)
