from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("country") == "US") & F.hour("started_at").between(8, 12))
    .select("id", "device", F.to_date("started_at").alias("started_at_date"))
)
