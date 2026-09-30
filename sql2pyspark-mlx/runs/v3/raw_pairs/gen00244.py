from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.to_date("started_at") < "2024-03-16")
    .withColumn("country_upper", F.upper("country"))
    .withColumn("clean_device", F.trim("device"))
    .orderBy(F.col("started_at").desc(), "country_upper")
)
