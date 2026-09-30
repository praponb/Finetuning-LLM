from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("started_at") >= "2024-12-01")
    .select("id", "browser")
    .orderBy(F.col("id").desc())
)
