from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.to_date("started_at") < "2024-05-01")
    .select("started_at")
    .orderBy("started_at")
)
