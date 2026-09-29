from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("country") == "US")
    .withColumn("country_upper", F.upper("country"))
    .withColumn("browser_filled", F.coalesce("browser", F.lit("Unknown")))
)
