from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("country") == "TH")
    .withColumn("country_filled", F.coalesce("country", F.lit("unassigned")))
    .withColumn("device_length", F.length("device"))
)
