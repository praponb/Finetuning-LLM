from pyspark.sql import functions as F

result = (
    df_sessions
    .select("id", "browser", "started_at")
    .orderBy("browser")
)
