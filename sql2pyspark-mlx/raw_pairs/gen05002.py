from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("started_at") < "2023-10-01")
    .select("started_at", "device", "country")
)
