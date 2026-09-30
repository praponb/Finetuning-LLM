from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("device") != "mobile")
    .withColumn("browser_prefix", F.substring("browser", 1, 2))
    .orderBy("started_at")
)
