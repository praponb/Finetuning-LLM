from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.hour("started_at").between(12, 15) & (F.col("device") != "desktop"))
    .select("id", "browser", "country", F.length("browser").alias("browser_length"))
)
