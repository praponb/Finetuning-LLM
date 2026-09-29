from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(~F.col("browser").isin(["Safari", "Edge", "Chrome"]))
    .select("id", "started_at")
    .orderBy("started_at")
)
