from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("started_at") >= "2023-12-01") | F.col("browser").isin(["Firefox", "Safari"]))
    .withColumn("country_browser", F.concat("country", F.lit(", "), "browser"))
    .withColumn("started_at_day", F.col("started_at").cast("date"))
)
