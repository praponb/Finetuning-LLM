from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("device") == "desktop")
    .withColumn("country_browser", F.concat("country", F.lit("/"), "browser"))
    .orderBy("started_at", "id")
    .limit(1)
)
