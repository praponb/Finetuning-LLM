from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser").isin(["Edge", "Firefox", "Safari"]))
    .select(
        "id",
        "started_at",
        "country",
        "browser",
        F.concat("browser", F.lit("/"), "device").alias("browser_device"),
    )
    .orderBy(F.col("browser_device").desc(), "id")
    .limit(5)
)
