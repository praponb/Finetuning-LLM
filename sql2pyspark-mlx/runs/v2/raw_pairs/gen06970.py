from pyspark.sql import functions as F

result = (
    df_sessions
    .withColumn("country_device", F.concat("country", F.lit("/"), "device"))
    .withColumn("started_at_label", F.date_format("started_at", "yyyy-MM"))
    .orderBy(F.col("id").desc())
    .limit(10)
)
