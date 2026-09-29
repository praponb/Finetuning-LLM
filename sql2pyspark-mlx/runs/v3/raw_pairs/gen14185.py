from pyspark.sql import functions as F

result = (
    df_sessions
    .withColumn("browser_country", F.concat("browser", F.lit("/"), "country"))
    .withColumn("clean_device", F.trim("device"))
)
