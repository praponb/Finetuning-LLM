from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("country") != "TH") & (F.col("device") != "mobile"))
    .select("id", "browser", "started_at")
)
