from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("device").isin(["desktop", "mobile", "tablet"]))
    .select("browser", "device")
    .intersect(
        df_sessions
        .filter(F.col("device") == "desktop")
        .select("browser", "device")
    )
    .orderBy("browser")
)
