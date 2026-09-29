from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.to_date("started_at") < "2023-12-01")
    .select("country", "browser", F.substring("device", 1, 3).alias("device_prefix"))
)
