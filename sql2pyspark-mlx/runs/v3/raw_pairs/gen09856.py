from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("device") == "desktop")
    .select("country", "browser")
    .distinct()
)
