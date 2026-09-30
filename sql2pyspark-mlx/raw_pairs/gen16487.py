from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(~F.col("country").isin(["DE", "JP", "US"]))
    .select("country", "started_at")
    .orderBy("country")
)
