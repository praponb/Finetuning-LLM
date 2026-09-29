from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(~F.col("browser").isin(["Safari", "Firefox", "Edge"]))
    .select(
        "id",
        "browser",
        F.when(F.col("browser") == "Safari", 1).otherwise(0).alias("is_safari"),
        F.lower("device").alias("device_lower"),
    )
    .orderBy(F.col("is_safari").desc(), "browser", "id")
    .limit(3)
)
