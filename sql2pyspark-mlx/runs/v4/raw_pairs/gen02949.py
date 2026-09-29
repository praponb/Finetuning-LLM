from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("started_at") >= "2023-11-09")
    .select("id", "device")
    .orderBy("id")
    .limit(1)
)
