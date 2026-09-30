from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("browser") == "Chrome") & F.col("browser").isin(["Edge", "Firefox"]))
    .withColumn("device_country", F.concat("device", F.lit(" "), "country"))
)
