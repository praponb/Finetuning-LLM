from pyspark.sql import functions as F

result = (
    df_sessions
    .select("country", "id")
    .intersectAll(
        df_sessions
        .filter(F.col("browser").isin(["Safari", "Firefox", "Chrome"]))
        .select("country", "id")
    )
)
