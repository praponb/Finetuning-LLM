from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.to_date("started_at") >= "2025-04-18")
    .select("country")
    .distinct()
    .orderBy(F.col("country").desc())
)
