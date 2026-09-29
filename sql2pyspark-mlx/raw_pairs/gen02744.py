from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(~F.col("country").isin(["US", "JP", "DE"]))
    .orderBy("started_at")
)
