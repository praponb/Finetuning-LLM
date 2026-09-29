from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser").isin(["Firefox", "Edge", "Chrome"]))
    .select("country", "browser", "device", F.trim("browser").alias("clean_browser"))
)
