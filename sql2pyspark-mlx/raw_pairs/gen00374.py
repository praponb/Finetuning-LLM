from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser").isin(["Chrome", "Edge", "Firefox"]) & F.col("country").isin(["US", "JP", "UK"]))
    .select("device", "started_at", F.upper("country").alias("country_upper"))
)
