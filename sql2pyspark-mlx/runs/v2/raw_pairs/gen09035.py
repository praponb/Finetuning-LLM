from pyspark.sql import functions as F

result = (
    df_sessions
    .select("device", "browser", F.to_date("started_at").alias("started_at_date"))
)
