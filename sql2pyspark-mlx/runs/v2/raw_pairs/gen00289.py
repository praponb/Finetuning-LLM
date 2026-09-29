from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("country") == "TH") & (F.col("started_at") < "2024-08-01"))
    .withColumn("started_at_hour", F.hour("started_at"))
)
