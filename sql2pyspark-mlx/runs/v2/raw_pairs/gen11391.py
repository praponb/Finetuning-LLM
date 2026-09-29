from pyspark.sql import functions as F

result = (
    df_sessions
    .withColumn("is_safari", F.when(F.col("browser") == "Safari", 1).otherwise(0))
    .withColumn("country_device", F.concat("country", F.lit(" - "), "device"))
)
